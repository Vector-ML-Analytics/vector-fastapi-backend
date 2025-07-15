"""
Multi-Tenant Database Manager for FastAPI Output Tables Service
Handles all output tables across all tenants with proper schema isolation
"""

import asyncio
import logging
from typing import Dict, List, Optional, Any, Union
from datetime import datetime
import asyncpg
from contextlib import asynccontextmanager
import time

from tenant_config import TenantConfigManager, TenantConfig, TableConfig
from config import db_config

logger = logging.getLogger(__name__)

class MultiTenantDatabaseError(Exception):
    """Custom exception for multi-tenant database operations"""
    pass

class MultiTenantDatabaseManager:
    """
    Multi-tenant database manager for output tables
    Provides schema-aware database operations with tenant isolation
    """
    
    def __init__(self):
        self.pool: Optional[asyncpg.Pool] = None
        self.connection_established = False
        self.tenant_pools: Dict[str, asyncpg.Pool] = {}
        self.cache = {}
        self.logger = logger
        
    async def init_pool(self):
        """Initialize database connection pool"""
        try:
            # Create main connection pool
            self.pool = await asyncpg.create_pool(
	         host=db_config.database_host,
	         port=db_config.database_port,
	         user=db_config.database_user,
                 password=db_config.database_password,
	         database=db_config.database_name,
                 min_size=2,
                 max_size=10,
	         command_timeout=60,                    # Increased from 30 to 60
	         timeout=10,                 # NEW - matches Django's connect_timeout
	         server_settings={
	               'application_name': 'fastapi_multi_tenant_output_service',
	                'search_path': 'public'            # NEW - matches Django's search_path
	         }
	    )            
            # Test connection
            async with self.pool.acquire() as conn:
                result = await conn.fetchrow('SELECT version()')
                logger.info(f"📊 Connected to: {result['version'][:80]}...")
                
            self.connection_established = True
            logger.info("✅ Multi-tenant database connection pool initialized successfully")
            
        except Exception as e:
            logger.error(f"❌ Failed to initialize database pool: {e}")
            raise MultiTenantDatabaseError(f"Database initialization failed: {e}")
    
    async def close_pool(self):
        """Close database connection pool"""
        try:
            if self.pool:
                await self.pool.close()
                self.pool = None
                
            # Close tenant-specific pools
            for tenant_pool in self.tenant_pools.values():
                await tenant_pool.close()
            self.tenant_pools.clear()
            
            self.connection_established = False
            logger.info("🔌 Multi-tenant database connection pool closed")
            
        except Exception as e:
            logger.error(f"❌ Error closing database pool: {e}")
    
    async def health_check(self) -> Dict[str, Any]:
        """Perform health check on database connections"""
        try:
            if not self.pool:
                return {"status": "unhealthy", "error": "No connection pool"}
            
            async with self.pool.acquire() as conn:
                # Test basic connectivity
                result = await conn.fetchrow('SELECT NOW() as current_time, version()')
                
                # Test tenant schemas
                tenant_status = {}
                for tenant_config in TenantConfigManager.get_all_tenants():
                    try:
                        schema_result = await conn.fetchrow(
                            "SELECT schema_name FROM information_schema.schemata WHERE schema_name = $1",
                            tenant_config.schema_name
                        )
                        tenant_status[tenant_config.company_url] = {
                            "schema_exists": schema_result is not None,
                            "schema_name": tenant_config.schema_name
                        }
                    except Exception as e:
                        tenant_status[tenant_config.company_url] = {
                            "schema_exists": False,
                            "error": str(e)
                        }
                
                return {
                    "status": "healthy",
                    "database_time": result['current_time'].isoformat(),
                    "database_version": result['version'][:50],
                    "connection_pool_size": self.pool.get_size(),
                    "tenants": tenant_status
                }
                
        except Exception as e:
            logger.error(f"❌ Database health check failed: {e}")
            return {"status": "unhealthy", "error": str(e)}
    
    async def get_template_id_from_load_datetime(self, company_url: str, load_datetime: str) -> Optional[str]:
        """
        Convert load_datetime to template_id for a specific tenant
        Mirrors Django's get_template_id_from_load_datetime method
        """
        tenant_config = TenantConfigManager.get_tenant_config(company_url)
        if not tenant_config:
            raise MultiTenantDatabaseError(f"No tenant configuration found for {company_url}")
        
        if not self.pool:
            raise MultiTenantDatabaseError("Database pool not initialized")
        
        try:
            async with self.pool.acquire() as conn:
                # Handle both 'T' and space separators in timestamps
                normalized_datetime = load_datetime.replace('T', ' ')
                
                # Try exact match first
                result = await conn.fetchrow(f"""
                    SELECT template_id, load_datetime 
                    FROM {tenant_config.schema_name}.portfolio_manager 
                    WHERE load_datetime::text LIKE $1 || '%'
                    LIMIT 1
                """, normalized_datetime)
                
                # If not found, try with date truncation
                if not result:
                    result = await conn.fetchrow(f"""
                        SELECT template_id, load_datetime 
                        FROM {tenant_config.schema_name}.portfolio_manager 
                        WHERE date_trunc('milliseconds', load_datetime::timestamp) = 
                              date_trunc('milliseconds', $1::timestamp)
                        LIMIT 1
                    """, normalized_datetime)
                
                if result:
                    template_id = str(result['template_id'])
                    logger.info(f"🔍 Template ID lookup for {company_url}: {load_datetime} → {template_id}")
                    return template_id
                else:
                    logger.warning(f"⚠️ No template_id found for {company_url} load_datetime: {load_datetime}")
                    return None
                    
        except Exception as e:
            logger.error(f"❌ Template ID lookup error for {company_url} {load_datetime}: {e}")
            raise MultiTenantDatabaseError(f"Template ID lookup failed: {e}")
    
    async def get_output_data(
        self,
        company_url: str,
        sheet_type: str,
        template_id: str,
        periods: int = 72,
        force_refresh: bool = False
    ) -> Optional[Dict[str, Any]]:
        """
        Get output data for a specific tenant and sheet type
        Uses the same PostgreSQL function as Django
        """
        tenant_config = TenantConfigManager.get_tenant_config(company_url)
        if not tenant_config:
            raise MultiTenantDatabaseError(f"No tenant configuration found for {company_url}")
        
        table_config = TenantConfigManager.get_table_config(company_url, sheet_type)
        if not table_config:
            raise MultiTenantDatabaseError(f"No table configuration found for {company_url}.{sheet_type}")
        
        if not self.pool:
            raise MultiTenantDatabaseError("Database pool not initialized")
        
        try:
            start_time = datetime.now()
            
            async with self.pool.acquire() as conn:
                # Call the same PostgreSQL function as Django with explicit parameter names
                result = await conn.fetchrow(
                    "SELECT public.generate_ag_grid_universal_output(p_schema_name := $1, p_portfolio_id := $2, p_sheet_type := $3, p_limit_periods := $4, app_code := $5) as result",
                    tenant_config.schema_name,
                    template_id,
                    table_config.sheet_id,
                    periods,
                    table_config.app_code
                )
                
                if not result or not result['result']:
                    logger.warning(f"⚠️ No data returned for {company_url}.{sheet_type} template {template_id}")
                    return None
                
                # Parse the JSON result
                import json
                data = json.loads(result['result'])
                
                # Add metadata
                if isinstance(data, dict):
                    data['metadata'] = {
                        'company_url': company_url,
                        'template_id': template_id,
                        'sheet_type': sheet_type,
                        'sheet_name': table_config.sheet_name,
                        'sheet_id': table_config.sheet_id,
                        'schema': tenant_config.schema_name,
                        'app_code': table_config.app_code,
                        'periods': periods,
                        'generated_at': datetime.now().isoformat(),
                        'execution_time_ms': (datetime.now() - start_time).total_seconds() * 1000,
                        'source': 'fastapi_multi_tenant',
                        'tenant_type': tenant_config.client_type.value,
                        'force_refresh': force_refresh
                    }
                
                execution_time = (datetime.now() - start_time).total_seconds() * 1000
                logger.info(f"⚡ {company_url}.{sheet_type} data retrieved in {execution_time:.2f}ms for template {template_id}")
                
                return data
                
        except Exception as e:
            logger.error(f"❌ Database error for {company_url}.{sheet_type} template {template_id}: {e}")
            raise MultiTenantDatabaseError(f"Data retrieval failed: {e}")
    
    async def get_all_output_data(
        self,
        company_url: str,
        template_id: str,
        periods: int = 72,
        sheet_types: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Get all output data for a tenant
        Returns multiple sheets in Django-compatible format
        """
        tenant_config = TenantConfigManager.get_tenant_config(company_url)
        if not tenant_config:
            raise MultiTenantDatabaseError(f"No tenant configuration found for {company_url}")
        
        # Use provided sheet types or get all available
        if sheet_types is None:
            sheet_types = [table.sheet_type for table in tenant_config.tables if table.is_active]
        
        # Validate sheet types
        valid_sheet_types = TenantConfigManager.get_supported_sheet_types(company_url)
        invalid_types = [st for st in sheet_types if st not in valid_sheet_types]
        if invalid_types:
            raise MultiTenantDatabaseError(f"Invalid sheet types for {company_url}: {invalid_types}")
        
        try:
            start_time = datetime.now()
            
            # Get data for all requested sheet types concurrently
            tasks = []
            for sheet_type in sheet_types:
                task = self.get_output_data(company_url, sheet_type, template_id, periods)
                tasks.append(task)
            
            # Wait for all tasks to complete
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Process results
            sheets = []
            errors = []
            
            for i, result in enumerate(results):
                sheet_type = sheet_types[i]
                
                if isinstance(result, Exception):
                    error_msg = f"Error getting {sheet_type}: {str(result)}"
                    logger.error(f"❌ {error_msg}")
                    errors.append(error_msg)
                elif result:
                    sheets.append(result)
                else:
                    logger.warning(f"⚠️ No data for {company_url}.{sheet_type}")
            
            execution_time = (datetime.now() - start_time).total_seconds() * 1000
            
            # Return Django-compatible response format
            response = {
                "sheets": sheets,
                "metadata": {
                    "company_url": company_url,
                    "template_id": template_id,
                    "tenant_name": tenant_config.display_name,
                    "schema": tenant_config.schema_name,
                    "client_type": tenant_config.client_type.value,
                    "periods": periods,
                    "sheet_count": len(sheets),
                    "requested_sheets": sheet_types,
                    "generated_at": datetime.now().isoformat(),
                    "execution_time_ms": execution_time,
                    "source": "fastapi_multi_tenant_all",
                    "errors": errors if errors else None
                }
            }
            
            logger.info(f"⚡ All {company_url} data retrieved in {execution_time:.2f}ms ({len(sheets)} sheets)")
            return response
            
        except Exception as e:
            logger.error(f"❌ Error getting all data for {company_url}: {e}")
            raise MultiTenantDatabaseError(f"All data retrieval failed: {e}")
    
    async def validate_portfolio(self, company_url: str, template_id: str) -> bool:
        """Validate if a portfolio exists for a specific tenant"""
        tenant_config = TenantConfigManager.get_tenant_config(company_url)
        if not tenant_config:
            return False
        
        if not self.pool:
            return False
        
        try:
            async with self.pool.acquire() as conn:
                result = await conn.fetchrow(f"""
                    SELECT COUNT(*) as count 
                    FROM {tenant_config.schema_name}.portfolio_manager 
                    WHERE template_id = $1
                """, template_id)
                
                return result and result['count'] > 0
                
        except Exception as e:
            logger.error(f"❌ Portfolio validation error for {company_url}.{template_id}: {e}")
            return False
    
    async def get_tenant_stats(self, company_url: str) -> Dict[str, Any]:
        """Get statistics for a specific tenant"""
        tenant_config = TenantConfigManager.get_tenant_config(company_url)
        if not tenant_config:
            raise MultiTenantDatabaseError(f"No tenant configuration found for {company_url}")
        
        if not self.pool:
            raise MultiTenantDatabaseError("Database pool not initialized")
        
        try:
            async with self.pool.acquire() as conn:
                # Get portfolio count
                portfolio_result = await conn.fetchrow(f"""
                    SELECT COUNT(*) as count 
                    FROM {tenant_config.schema_name}.portfolio_manager
                """)
                
                # Get table information
                table_stats = []
                for table_config in tenant_config.tables:
                    if table_config.is_active:
                        try:
                            table_result = await conn.fetchrow(f"""
                                SELECT COUNT(*) as count 
                                FROM {tenant_config.schema_name}."{table_config.data_table}"
                            """)
                            table_stats.append({
                                'sheet_type': table_config.sheet_type,
                                'sheet_name': table_config.sheet_name,
                                'data_table': table_config.data_table,
                                'row_count': table_result['count'] if table_result else 0
                            })
                        except Exception as e:
                            table_stats.append({
                                'sheet_type': table_config.sheet_type,
                                'sheet_name': table_config.sheet_name,
                                'data_table': table_config.data_table,
                                'row_count': 0,
                                'error': str(e)
                            })
                
                return {
                    'company_url': company_url,
                    'tenant_name': tenant_config.display_name,
                    'schema': tenant_config.schema_name,
                    'client_type': tenant_config.client_type.value,
                    'portfolio_count': portfolio_result['count'] if portfolio_result else 0,
                    'table_count': len(tenant_config.tables),
                    'active_tables': len([t for t in tenant_config.tables if t.is_active]),
                    'table_stats': table_stats,
                    'features': tenant_config.features,
                    'generated_at': datetime.now().isoformat()
                }
                
        except Exception as e:
            logger.error(f"❌ Error getting tenant stats for {company_url}: {e}")
            raise MultiTenantDatabaseError(f"Tenant stats retrieval failed: {e}")
    
    async def get_financial_data(
        self,
        company_url: str,
        template_id: str,
        sheet_filter: Optional[str] = None,
        periods: int = 72,
        force_refresh: bool = False
    ) -> Optional[Dict[str, Any]]:
        """
        Get financial data for AG Grid tables
        Mirrors Django's PostgreSQLDynamicAPI endpoint structure
        Uses generate_ag_grid_universal_optimized PostgreSQL function for INPUT tables
        Returns all sheets when no sheet_filter is provided (Django-compatible format)
        """
        cache_key = f"financial_data:{company_url}:{template_id}:{sheet_filter}:{periods}"
        
        # Check cache first unless force refresh
        if not force_refresh and cache_key in self.cache:
            logger.info(f"📋 Cache hit for financial data {company_url}.{template_id}")
            return self.cache[cache_key]

        try:
            # Get tenant configuration
            tenant_config = TenantConfigManager.get_tenant_config(company_url)
            if not tenant_config:
                raise MultiTenantDatabaseError(f"Tenant {company_url} not found")

            # If no sheet filter provided, return all available sheets (Django-compatible format)
            if not sheet_filter:
                return await self._get_all_financial_data_sheets(company_url, template_id, periods, force_refresh)

            # Handle specific sheet filter - validate it exists in input tables
            input_table_config = TenantConfigManager.get_input_table_config(company_url, sheet_filter)
            if not input_table_config:
                # Try to map common short names to full names for backward compatibility
                sheet_mapping = {
                    'bs': 'balance_sheet',
                    'is': 'income_statement', 
                    'cf': 'cash_flow',
                    'broker': 'Broker',
                    'credit': 'Credit',
                    'deposit': 'Deposit',
                    'growth': 'Growth'
                }
                mapped_sheet = sheet_mapping.get(sheet_filter.lower())
                if mapped_sheet:
                    input_table_config = TenantConfigManager.get_input_table_config(company_url, mapped_sheet)
                    if input_table_config:
                        sheet_filter = mapped_sheet
                    
                    if not input_table_config:
                        available_sheets = TenantConfigManager.get_input_sheet_types(company_url)
                        raise MultiTenantDatabaseError(
                            f"Sheet type '{sheet_filter}' not found for {company_url}. "
                            f"Available input sheets: {available_sheets}"
                        )

            # Get single sheet data
            single_sheet = await self._get_single_financial_sheet(company_url, template_id, sheet_filter, periods)
            
            # Cache and return single sheet
            if single_sheet:
                self.cache[cache_key] = single_sheet
                logger.info(f"✅ Financial data retrieved for {company_url} template {template_id} sheet {sheet_filter}")
            
            return single_sheet
            
        except Exception as e:
            error_msg = f"Failed to get financial data for {company_url}.{template_id} sheet {sheet_filter}: {str(e)}"
            logger.error(f"❌ {error_msg}")
            raise MultiTenantDatabaseError(error_msg)

    async def _get_all_financial_data_sheets(
        self,
        company_url: str,
        template_id: str,
        periods: int = 72,
        force_refresh: bool = False
    ) -> Dict[str, Any]:
        """
        Get all available financial data sheets for a tenant
        Returns Django-compatible format with sheets array
        """
        try:
            # Get tenant configuration
            tenant_config = TenantConfigManager.get_tenant_config(company_url)
            if not tenant_config:
                raise MultiTenantDatabaseError(f"Tenant {company_url} not found")

            # Get all available input sheet types
            available_sheets = TenantConfigManager.get_input_sheet_types(company_url)
            
            if not available_sheets:
                logger.warning(f"⚠️ No input sheets configured for {company_url}")
                return {
                    "sheets": [],
                    "metadata": {
                        "company_url": company_url,
                        "template_id": template_id,
                        "tenant_name": tenant_config.display_name,
                        "schema": tenant_config.schema_name,
                        "client_type": tenant_config.client_type.value,
                        "periods": periods,
                        "sheet_count": 0,
                        "generated_at": datetime.now().isoformat(),
                        "source": "fastapi_multi_tenant_financial_data",
                        "message": "No input sheets configured"
                    }
                }

            # Get data for all sheets concurrently
            start_time = datetime.now()
            tasks = []
            
            for sheet_type in available_sheets:
                task = self._get_single_financial_sheet(company_url, template_id, sheet_type, periods)
                tasks.append(task)
            
            # Wait for all tasks to complete
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Process results
            sheets = []
            errors = []
            
            for i, result in enumerate(results):
                sheet_type = available_sheets[i]
                
                if isinstance(result, Exception):
                    error_msg = f"Error getting {sheet_type}: {str(result)}"
                    logger.error(f"❌ {error_msg}")
                    errors.append(error_msg)
                elif result:
                    sheets.append(result)
                else:
                    logger.warning(f"⚠️ No data for {company_url}.{sheet_type}")

            execution_time = (datetime.now() - start_time).total_seconds() * 1000
            
            # Return Django-compatible response format
            response = {
                "sheets": sheets,
                "metadata": {
                    "company_url": company_url,
                    "template_id": template_id,
                    "tenant_name": tenant_config.display_name,
                    "schema": tenant_config.schema_name,
                    "client_type": tenant_config.client_type.value,
                    "periods": periods,
                    "sheet_count": len(sheets),
                    "requested_sheets": available_sheets,
                    "generated_at": datetime.now().isoformat(),
                    "execution_time_ms": execution_time,
                    "source": "fastapi_multi_tenant_financial_data",
                    "table_type": "input_tables",
                    "function_used": "generate_ag_grid_universal_optimized",
                    "errors": errors if errors else None
                }
            }
            
            logger.info(f"⚡ All {company_url} financial data retrieved in {execution_time:.2f}ms ({len(sheets)} sheets)")
            return response
            
        except Exception as e:
            logger.error(f"❌ Error getting all financial data for {company_url}: {e}")
            raise MultiTenantDatabaseError(f"All financial data retrieval failed: {e}")

    async def _get_single_financial_sheet(
        self,
        company_url: str,
        template_id: str,
        sheet_filter: str,
        periods: int = 72
    ) -> Optional[Dict[str, Any]]:
        """
        Get financial data for a single sheet
        """
        try:
            # Get tenant configuration
            tenant_config = TenantConfigManager.get_tenant_config(company_url)
            if not tenant_config:
                raise MultiTenantDatabaseError(f"Tenant {company_url} not found")

            # Get database connection
            async with self.pool.acquire() as conn:
                # Set the search path to the tenant's schema
                await conn.execute(f"SET search_path TO {tenant_config.schema_name}")
                
                # Call the PostgreSQL function for INPUT tables
                # This is different from output tables - uses generate_ag_grid_universal_optimized
                query = """
                    SELECT public.generate_ag_grid_universal_optimized(
                        p_schema_name := $1::text,
                        p_portfolio_id := $2::text,
                        p_sheet_type := $3::text,
                        p_limit_periods := $4::integer
                    ) as result
                """
                
                # Execute the query
                logger.info(f"🔍 Executing financial data query for {company_url} template {template_id} sheet {sheet_filter}")
                result = await conn.fetchrow(query, tenant_config.schema_name, template_id, sheet_filter, periods)
                
                if not result or not result['result']:
                    logger.warning(f"⚠️ No financial data found for {company_url} template {template_id} sheet {sheet_filter}")
                    return None
                
                # Parse the JSON result
                import json
                if isinstance(result['result'], str):
                    financial_data = json.loads(result['result'])
                else:
                    financial_data = result['result']
                
                # Add metadata
                if isinstance(financial_data, dict):
                    financial_data['metadata'] = {
                        'company_url': company_url,
                        'template_id': template_id,
                        'sheet_filter': sheet_filter,
                        'schema': tenant_config.schema_name,
                        'periods': periods,
                        'generated_at': datetime.now().isoformat(),
                        'source': 'fastapi_multi_tenant_financial_data',
                        'tenant_type': tenant_config.client_type.value,
                        'table_type': 'input_tables',
                        'function_used': 'generate_ag_grid_universal_optimized'
                    }
                
                logger.info(f"✅ Financial data retrieved for {company_url} template {template_id} sheet {sheet_filter}")
                return financial_data
            
        except Exception as e:
            logger.error(f"❌ Error getting single financial sheet {sheet_filter} for {company_url}: {e}")
            return None

    async def update_bulk_financial_data(
        self,
        company_url: str,
        template_id: str,
        changes: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Optimized bulk updates for financial data with minimal database operations.
        Equivalent to Django's update_bulk endpoint.
        """
        self.logger.info(f"🔥 UPDATE_BULK called for company {company_url}")
        self.logger.info(f"🔥 Template ID: {template_id}, Changes count: {len(changes) if changes else 0}")
        
        # Get tenant configuration
        tenant_config = TenantConfigManager.get_tenant_config(company_url)
        if not tenant_config:
            raise MultiTenantDatabaseError(f"Invalid company_url: {company_url}")
        
        # Pre-validate all changes have required fields (fail fast)
        required_fields = {'new_value', 'rowID', 'column_name', 'sheet_name'}
        for i, update in enumerate(changes):
            missing = required_fields - set(update.keys())
            if missing:
                raise MultiTenantDatabaseError(
                    f'Missing required fields in update {i}: {", ".join(missing)}'
                )
        
        # Group updates by (sheet_name, column_name) for batch processing
        # This eliminates redundant column mapping and database queries
        update_groups = {}
        updated_sheets = set()
        sheet_id_to_table_name = {}  # Cache for sheet ID to table name mapping
        
        for update in changes:
            sheet_id = update['sheet_name']  # This is actually the sheet ID
            row_id = update['rowID']
            column_name = update['column_name']
            
            # Map sheet ID to actual table name
            if sheet_id not in sheet_id_to_table_name:
                mapped_table_name = self._get_table_name_from_sheet_id(company_url, sheet_id)
                sheet_id_to_table_name[sheet_id] = mapped_table_name
                self.logger.info(f"Mapped sheet_id '{sheet_id}' to table_name '{mapped_table_name}' for company '{company_url}'")
            
            table_name = sheet_id_to_table_name[sheet_id]
            updated_sheets.add(sheet_id)  # Keep original sheet ID for tracking
            
            # Format date once (avoid repeated string operations)
            month, year = column_name.split('-')
            formatted_date = f"{month.capitalize()}-{year}"
            
            # Group by (table_name, column) to minimize database operations
            key = (table_name, row_id)
            if key not in update_groups:
                update_groups[key] = []
            
            update_groups[key].append({
                'date': formatted_date,
                'value': update['new_value']
            })
        
        # Execute database operations
        results = []
        errors = []
        total_updated = 0
        
        async with self.pool.acquire() as conn:
            try:
                for (table_name, column_name), updates in update_groups.items():
                    try:
                        # Build optimized batch update with single query per column
                        dates = [update['date'] for update in updates]
                        values = [update['value'] for update in updates]
                        
                        # Single optimized query with CASE statement for all dates at once
                        case_conditions = []
                        query_params = []
                        param_counter = 1
                        
                        for date, value in zip(dates, values):
                            case_conditions.append(f"WHEN date_value = TO_DATE(${param_counter}, 'Mon-YY') THEN ${param_counter + 1}")
                            query_params.extend([date, value])
                            param_counter += 2
                        
                        # Add final parameters
                        query_params.extend([template_id])
                        template_param = param_counter
                        param_counter += 1
                        
                        # Add dates array for WHERE clause
                        date_placeholders = []
                        for date in dates:
                            date_placeholders.append(f"TO_DATE(${param_counter}, 'Mon-YY')")
                            query_params.append(date)
                            param_counter += 1
                        
                        # Execute single optimized update query
                        update_query = f"""
                            UPDATE {tenant_config.schema_name}."{table_name}"
                            SET "{column_name}" = CASE {' '.join(case_conditions)} ELSE "{column_name}" END,
                                updated_at = CURRENT_TIMESTAMP
                            WHERE portfolio_id = ${template_param}
                            AND date_value IN ({', '.join(date_placeholders)})
                            RETURNING date_value, "{column_name}" as updated_value;
                        """
                        
                        self.logger.info(f"Executing SQL: UPDATE {tenant_config.schema_name}.\"{table_name}\" SET \"{column_name}\" = ... WHERE portfolio_id = {template_id}")
                        
                        updated_rows = await conn.fetch(update_query, *query_params)
                        rows_updated = len(updated_rows)
                        total_updated += rows_updated
                        
                        # Process results efficiently
                        for result in updated_rows:
                            results.append({
                                'table_name': table_name,
                                'date': str(result['date_value']),
                                'updated_value': float(result['updated_value']),
                                'column_name': column_name
                            })
                        
                        # Track errors only if no rows updated
                        if not rows_updated:
                            errors.append({
                                'table_name': table_name,
                                'column_name': column_name,
                                'error': 'No matching rows found'
                            })
                            
                    except Exception as e:
                        self.logger.error(f"Error updating {table_name}.{column_name}: {str(e)}")
                        errors.append({
                            'table_name': table_name,
                            'column_name': column_name,
                            'error': str(e)
                        })
                        
            except Exception as e:
                self.logger.error(f"Database connection error during bulk update: {str(e)}")
                raise MultiTenantDatabaseError(f"Database error during bulk update: {str(e)}")
        
        # Cache invalidation (simplified for FastAPI)
        cache_stats = {}
        if total_updated > 0:
            try:
                # Clear relevant cache entries
                cache_keys_to_clear = [
                    f"{company_url}:financial_data:{template_id}",
                    f"{company_url}:financial_data:{template_id}:*"
                ]
                
                # Clear cache entries
                cleared_keys = []
                for key in cache_keys_to_clear:
                    if key in self.cache:
                        del self.cache[key]
                        cleared_keys.append(key)
                
                cache_stats = {
                    'keys_cleared': len(cleared_keys),
                    'patterns_cleared': 0,
                    'cache_types': ['financial_data'],
                    'failed_operations': 0,
                    'cleared_keys': cleared_keys,
                    'cleared_patterns': [],
                }
                
            except Exception as cache_error:
                self.logger.error(f"Cache invalidation failed: {cache_error}")
                cache_stats = {
                    'keys_cleared': 0,
                    'patterns_cleared': 0,
                    'cache_types': [],
                    'failed_operations': 1,
                    'cleared_keys': [],
                    'cleared_patterns': [],
                    'error': str(cache_error)
                }
        else:
            cache_stats = {
                'keys_cleared': 0,
                'patterns_cleared': 0,
                'cache_types': [],
                'failed_operations': 0,
                'cleared_keys': [],
                'cleared_patterns': [],
                'message': 'No cache clearing needed - no updates were made'
            }
        
        self.logger.info(f"🔥 UPDATE_BULK completing with {total_updated} rows updated")
        
        return {
            'success': total_updated > 0,
            'message': f'Updated {total_updated} rows' + (f' with {len(errors)} errors' if errors else ''),
            'rows_updated': total_updated,
            'sheets_updated': len(updated_sheets),
            'updated_sheets': list(updated_sheets),
            'errors': errors if errors else None,
            'cache_stats': cache_stats,
            'results': results
        }
    
    def _get_table_name_from_sheet_id(self, company_url: str, sheet_id: str) -> str:
        """
        Map sheet ID to actual table name using tenant configuration.
        """
        tenant_config = TenantConfigManager.get_tenant_config(company_url)
        if not tenant_config:
            raise MultiTenantDatabaseError(f"Invalid company_url: {company_url}")
        
        # Look in input tables first (for financial data)
        for table in tenant_config.input_tables:
            if table.sheet_id == sheet_id:
                return table.data_table
        
        # Fallback to output tables
        for table in tenant_config.tables:
            if table.sheet_id == sheet_id:
                return table.data_table
        
        # If not found, return sheet_id as table name (fallback)
        self.logger.warning(f"Sheet ID '{sheet_id}' not found in tenant config for {company_url}, using as table name")
        return sheet_id

# Create global instance
multi_tenant_db = MultiTenantDatabaseManager() 

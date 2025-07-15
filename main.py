#!/usr/bin/env python3
"""
Multi-Tenant FastAPI Output Tables Service
High-performance service for ALL output tables across ALL tenants
Supports the same multi-tenant architecture as Django with WebSocket support
"""

import asyncio
import logging
import json
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Dict, Any, Optional, List, Set, Union

from fastapi import FastAPI, HTTPException, Query, Path, APIRouter, WebSocket, WebSocketDisconnect, Request, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel

# Import our modules
from config import fastapi_config, validate_environment
from tenant_config import TenantConfigManager, TenantConfig
from multi_tenant_database import multi_tenant_db, MultiTenantDatabaseError
from auth import JWTValidationError, get_current_user, get_authenticated_user_for_company, AuthenticatedUser

# Set up logging
logging.basicConfig(
    level=getattr(logging, fastapi_config.log_level.upper()),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# WebSocket Connection Manager
class WebSocketConnectionManager:
    """Manages WebSocket connections for real-time updates"""
    
    def __init__(self):
        # Store active connections by company_url
        self.active_connections: Dict[str, Set[WebSocket]] = {}
        # Store connection metadata
        self.connection_metadata: Dict[WebSocket, Dict[str, Any]] = {}
    
    async def connect(self, websocket: WebSocket, company_url: str, client_id: str = None):
        """Accept a new WebSocket connection"""
        await websocket.accept()
        
        # Initialize company connections if not exists
        if company_url not in self.active_connections:
            self.active_connections[company_url] = set()
        
        # Add connection
        self.active_connections[company_url].add(websocket)
        
        # Store metadata
        self.connection_metadata[websocket] = {
            "company_url": company_url,
            "client_id": client_id,
            "connected_at": datetime.now().isoformat(),
            "last_ping": datetime.now().isoformat()
        }
        
        logger.info(f"🔌 WebSocket connected: {company_url} (client: {client_id})")
        
        # Send welcome message
        await self.send_personal_message({
            "type": "connection_established",
            "company_url": company_url,
            "client_id": client_id,
            "timestamp": datetime.now().isoformat(),
            "message": "WebSocket connection established"
        }, websocket)
    
    def disconnect(self, websocket: WebSocket):
        """Remove a WebSocket connection"""
        metadata = self.connection_metadata.get(websocket, {})
        company_url = metadata.get("company_url")
        client_id = metadata.get("client_id")
        
        if company_url and websocket in self.active_connections.get(company_url, set()):
            self.active_connections[company_url].discard(websocket)
            
            # Clean up empty company sets
            if not self.active_connections[company_url]:
                del self.active_connections[company_url]
        
        # Remove metadata
        self.connection_metadata.pop(websocket, None)
        
        logger.info(f"🔌 WebSocket disconnected: {company_url} (client: {client_id})")
    
    async def send_personal_message(self, message: Dict[str, Any], websocket: WebSocket):
        """Send a message to a specific WebSocket connection"""
        try:
            await websocket.send_text(json.dumps(message))
        except Exception as e:
            logger.error(f"❌ Failed to send WebSocket message: {e}")
            self.disconnect(websocket)
    
    async def broadcast_to_company(self, message: Dict[str, Any], company_url: str):
        """Broadcast a message to all connections for a specific company"""
        connections = self.active_connections.get(company_url, set()).copy()
        
        if not connections:
            logger.debug(f"📡 No WebSocket connections for {company_url}")
            return
        
        logger.info(f"📡 Broadcasting to {len(connections)} connections for {company_url}")
        
        # Send to all connections concurrently
        tasks = []
        for connection in connections:
            task = self.send_personal_message(message, connection)
            tasks.append(task)
        
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
    
    async def broadcast_to_all(self, message: Dict[str, Any]):
        """Broadcast a message to all active connections"""
        all_connections = set()
        for connections in self.active_connections.values():
            all_connections.update(connections)
        
        if not all_connections:
            logger.debug("📡 No active WebSocket connections")
            return
        
        logger.info(f"📡 Broadcasting to {len(all_connections)} total connections")
        
        # Send to all connections concurrently
        tasks = []
        for connection in all_connections:
            task = self.send_personal_message(message, connection)
            tasks.append(task)
        
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
    
    def get_connection_stats(self) -> Dict[str, Any]:
        """Get statistics about active connections"""
        total_connections = sum(len(connections) for connections in self.active_connections.values())
        
        company_stats = {}
        for company_url, connections in self.active_connections.items():
            company_stats[company_url] = {
                "connection_count": len(connections),
                "client_ids": [
                    self.connection_metadata.get(conn, {}).get("client_id", "unknown")
                    for conn in connections
                ]
            }
        
        return {
            "total_connections": total_connections,
            "companies_connected": len(self.active_connections),
            "company_stats": company_stats,
            "timestamp": datetime.now().isoformat()
        }

# Create global WebSocket manager
websocket_manager = WebSocketConnectionManager()

# Pydantic models for request/response validation
class TenantInfo(BaseModel):
    company_url: str
    company_name: str
    schema_name: str
    display_name: str
    client_type: str
    table_count: int
    active_tables: int
    features: List[str]

class SheetInfo(BaseModel):
    sheet_type: str
    sheet_name: str
    sheet_id: str
    app_code: str
    is_active: bool

class OutputDataRequest(BaseModel):
    periods: int = 72
    force_refresh: bool = False
    sheet_types: Optional[List[str]] = None

class BulkUpdateChange(BaseModel):
    new_value: float
    rowID: str
    column_name: str
    sheet_name: str

class BulkUpdateRequest(BaseModel):
    changes: List[BulkUpdateChange]

# Validation response models
class ValidationError(BaseModel):
    """Individual validation error"""
    portfolio_id: str
    validation_rule_name: str
    is_valid: bool
    error_message: str
    actual_values: Optional[Any] = None

class ValidationStatusResponse(BaseModel):
    """Response model for validation status endpoint"""
    status: Optional[str] = None
    message: Optional[str] = None
    # Dynamic fields for each sheet name with list of validation errors
    # e.g., "Broker Mix %": [ValidationError, ...]
    
    class Config:
        extra = "allow"  # Allow additional fields for dynamic sheet names

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Manage the lifecycle of the FastAPI application
    Handles startup and shutdown tasks
    """
    # Startup
    logger.info("🚀 Starting Multi-Tenant FastAPI Output Tables Service...")
    
    try:
        # Validate environment configuration
        validate_environment()
        
        # Initialize database connection pool
        await multi_tenant_db.init_pool()
        
        # Log startup success
        logger.info("✅ Multi-tenant FastAPI service started successfully")
        logger.info(f"🔗 Database connected: {multi_tenant_db.connection_established}")
        
        # Log tenant configurations
        tenants = TenantConfigManager.get_all_tenants()
        logger.info(f"📊 Loaded {len(tenants)} tenant configurations:")
        for tenant in tenants:
            logger.info(f"  - {tenant.company_url}: {tenant.display_name} ({len(tenant.tables)} tables)")
        
        yield
        
    except Exception as e:
        logger.error(f"❌ Failed to start multi-tenant service: {e}")
        raise
    
    finally:
        # Shutdown
        logger.info("🛑 Shutting down Multi-Tenant FastAPI service...")
        
        # Close database connections
        await multi_tenant_db.close_pool()
        
        logger.info("✅ Multi-tenant FastAPI service shutdown complete")

# Create the FastAPI application
app = FastAPI(
    title="Multi-Tenant Output Tables Service",
    description="High-performance service for ALL output tables across ALL tenants",
    version="2.0.0",
    lifespan=lifespan
)

# Add CORS middleware for frontend integration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:3001", "https://www.vector-portal.com"],
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["*"],
)

# Add JWT exception handler
@app.exception_handler(JWTValidationError)
async def jwt_validation_exception_handler(request: Request, exc: JWTValidationError):
    """Handle JWT validation errors and return appropriate HTTP responses"""
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "detail": exc.message,
            "type": "authentication_error",
            "timestamp": datetime.now().isoformat()
        }
)

# Create router with /api/fastapi prefix
fastapi_router = APIRouter(prefix="/api/fastapi")

# Health check endpoint
@app.get("/health", tags=["Health"])
async def health_check():
    """
    Health check endpoint to verify service status
    """
    try:
        # Check database health
        db_health = await multi_tenant_db.health_check()
        
        # Get tenant configurations
        tenants = TenantConfigManager.get_all_tenants()
        
        # Overall health status
        is_healthy = (
            db_health.get('status') == 'healthy' and 
            multi_tenant_db.connection_established and
            len(tenants) > 0
        )
        
        return {
            "status": "healthy" if is_healthy else "unhealthy",
            "service": "multi_tenant_output_tables",
            "timestamp": datetime.now().isoformat(),
            "database": db_health,
            "tenants": {
                "count": len(tenants),
                "active": len([t for t in tenants if t.is_active]),
                "configurations": [
                    {
                        "company_url": t.company_url,
                        "display_name": t.display_name,
                        "schema": t.schema_name,
                        "client_type": t.client_type.value,
                        "table_count": len(t.tables),
                        "active_tables": len([table for table in t.tables if table.is_active])
                    }
                    for t in tenants
                ]
            }
        }
        
    except Exception as e:
        logger.error(f"❌ Health check failed: {e}")
        raise HTTPException(status_code=500, detail=f"Health check failed: {str(e)}")

# Tenant discovery endpoints
@fastapi_router.get("/tenants", response_model=List[TenantInfo], tags=["Tenants"])
async def get_all_tenants():
    """
    Get all available tenant configurations
    """
    try:
        tenants = TenantConfigManager.get_all_tenants()
        return [
            TenantInfo(
                company_url=tenant.company_url,
                company_name=tenant.company_name,
                schema_name=tenant.schema_name,
                display_name=tenant.display_name,
                client_type=tenant.client_type.value,
                table_count=len(tenant.tables),
                active_tables=len([t for t in tenant.tables if t.is_active]),
                features=tenant.features
            )
            for tenant in tenants
        ]
    except Exception as e:
        logger.error(f"❌ Error getting tenants: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get tenants: {str(e)}")

@fastapi_router.get("/tenants/{company_url}", response_model=TenantInfo, tags=["Tenants"])
async def get_tenant_info(company_url: str = Path(..., description="Company URL identifier")):
    """
    Get detailed information about a specific tenant
    """
    try:
        tenant_config = TenantConfigManager.get_tenant_config(company_url)
        if not tenant_config:
            raise HTTPException(status_code=404, detail=f"Tenant {company_url} not found")
        
        return TenantInfo(
            company_url=tenant_config.company_url,
            company_name=tenant_config.company_name,
            schema_name=tenant_config.schema_name,
            display_name=tenant_config.display_name,
            client_type=tenant_config.client_type.value,
            table_count=len(tenant_config.tables),
            active_tables=len([t for t in tenant_config.tables if t.is_active]),
            features=tenant_config.features
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Error getting tenant {company_url}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get tenant info: {str(e)}")

@fastapi_router.get("/tenants/{company_url}/tables", response_model=List[SheetInfo], tags=["Tenants"])
async def get_tenant_tables(company_url: str = Path(..., description="Company URL identifier")):
    """
    Get all available tables/sheets for a specific tenant
    """
    try:
        tenant_config = TenantConfigManager.get_tenant_config(company_url)
        if not tenant_config:
            raise HTTPException(status_code=404, detail=f"Tenant {company_url} not found")
        
        return [
            SheetInfo(
                sheet_type=table.sheet_type,
                sheet_name=table.sheet_name,
                sheet_id=table.sheet_id,
                app_code=table.app_code,
                is_active=table.is_active
            )
            for table in tenant_config.tables
        ]
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Error getting tables for {company_url}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get tenant tables: {str(e)}")

@fastapi_router.get("/tenants/{company_url}/stats", tags=["Tenants"])
async def get_tenant_stats(company_url: str = Path(..., description="Company URL identifier")):
    """
    Get detailed statistics for a specific tenant
    """
    try:
        stats = await multi_tenant_db.get_tenant_stats(company_url)
        return stats
    except MultiTenantDatabaseError as e:
        if "not found" in str(e):
            raise HTTPException(status_code=404, detail=str(e))
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        logger.error(f"❌ Error getting stats for {company_url}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get tenant stats: {str(e)}")

# Main output data endpoints
@fastapi_router.get("/company/{company_url}/output-data/{sheet_type}/", tags=["Output Data"])
async def get_output_data(
    company_url: str = Path(..., description="Company URL identifier"),
    sheet_type: str = Path(..., description="Sheet type (e.g., amort-all, bs, is, cf)"),
    start_timestamp: str = Query(..., description="Start timestamp for template lookup"),
    end_timestamp: str = Query(..., description="End timestamp for template lookup"),
    periods: int = Query(72, description="Number of periods to retrieve"),
    force_refresh: bool = Query(False, description="Force refresh from database"),
    current_user: AuthenticatedUser = Depends(get_current_user)
):
    """
    Get output data for a specific sheet type
    Mirrors Django's output-data endpoint structure
    """
    try:
        logger.info(f"📊 Output data request for {company_url}.{sheet_type} by user {current_user.email}")
        
        # Validate company access
        if current_user.company_url != company_url:
            logger.warning(f"⚠️ User {current_user.email} attempted to access {company_url} but belongs to {current_user.company_url}")
            raise HTTPException(
                status_code=403,
                detail=f"Access denied: You don't have permission to access {company_url}"
            )
        
        # Get tenant configuration
        tenant_config = TenantConfigManager.get_tenant_config(company_url)
        if not tenant_config:
            raise HTTPException(status_code=404, detail=f"Tenant {company_url} not found")
        
        # Convert timestamp to template_id
        template_id = await multi_tenant_db.get_template_id_from_load_datetime(company_url, start_timestamp)
        if not template_id:
            raise HTTPException(status_code=404, detail=f"No template found for timestamp {start_timestamp}")
        
        # Get output data
        result = await multi_tenant_db.get_output_data(
            company_url=company_url,
            sheet_type=sheet_type,
            template_id=template_id,
                            periods=periods,
                            force_refresh=force_refresh
                        )
                        
        if not result:
            raise HTTPException(status_code=404, detail=f"No data found for {sheet_type}")
        
        logger.info(f"📡 FastAPI response sent for {company_url}.{sheet_type} template {template_id}")
        sheet_data = {
            "metadata": {
                "generated_at": datetime.now().isoformat(),
                "total_sheets": 1,
                "portfolio_id": template_id,
                "schema": tenant_config.schema_name,
                "generator_version": "2.0",
                "request_params": {
                    "sheet_type": sheet_type,
                    "limit_periods": 72,
                    "app_code": result['metadata']['app_code'],
                    "force_refresh": False
                },
                "api_version": "1.0",
                "endpoint": "universal_output_api",
                "cache_status": "generated"
            },
            "sheets": [result]
        }
        return sheet_data
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Error in get_output_data: {e}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

# Financial data endpoint - mirrors Django's ag_grid_portfolio/financial-data API
@fastapi_router.get("/company/{company_url}/financial-data/", tags=["Financial Data"])
async def get_financial_data(
    company_url: str = Path(..., description="Company URL identifier"),
    template_id: str = Query(..., description="Template/Portfolio ID"),
    portfolio_id: str = Query(None, description="Portfolio ID (alias for template_id)"),
    sheet: str = Query(None, description="Specific sheet to retrieve"),
    periods: int = Query(72, description="Number of periods to retrieve"),
    force_refresh: bool = Query(False, description="Force refresh from database"),
    cache_bust: bool = Query(False, description="Cache busting for development"),
    current_user: AuthenticatedUser = Depends(get_current_user)
):
    """
    Get financial data for AG Grid tables (INPUT tables)
    Mirrors Django's PostgreSQLDynamicAPI endpoint structure
    Returns single-sheet JSON format for input tables
    """
    try:
        logger.info(f"📊 Financial data request for {company_url}, template {template_id} by user {current_user.email}")
        
        # Validate company access
        if current_user.company_url != company_url:
            logger.warning(f"⚠️ User {current_user.email} attempted to access {company_url} but belongs to {current_user.company_url}")
            raise HTTPException(
                status_code=403,
                detail=f"Access denied: You don't have permission to access {company_url}"
            )

        tenant_config = TenantConfigManager.get_tenant_config(company_url)
        if not tenant_config:
            raise HTTPException(status_code=404, detail=f"Tenant {company_url} not found")
        
        # Use portfolio_id if template_id is not provided
        if not template_id and portfolio_id:
            template_id = portfolio_id
        
        # Get financial data using the input tables structure
        result = await multi_tenant_db.get_financial_data(
            company_url=company_url,
            template_id=template_id,
            sheet_filter=sheet,
            periods=periods,
            force_refresh=force_refresh
        )
        
        if not result:
            available_sheets = TenantConfigManager.get_input_sheet_types(company_url)
            raise HTTPException(
                status_code=404, 
                detail=f"No financial data found for template {template_id}. Available sheets: {available_sheets}"
            )
        
        # For input tables, the result is a single sheet, not an array
        # If a specific sheet was requested, validate it matches
        if sheet and isinstance(result, dict) and result.get('id') != sheet:
            # Try to map short names to full names for validation
            sheet_mapping = {
                'bs': 'balance_sheet',
                'is': 'income_statement', 
                'cf': 'cash_flow',
                'broker': 'Broker',
                'credit': 'Credit',
                'deposit': 'Deposit',
                'growth': 'Growth'
            }
            mapped_sheet = sheet_mapping.get(sheet.lower(), sheet)
            
            if result.get('id') != mapped_sheet:
                available_sheets = TenantConfigManager.get_input_sheet_types(company_url)
                raise HTTPException(
                    status_code=404, 
                    detail=f"Sheet '{sheet}' not found. Available sheets: {available_sheets}"
                )
        
        logger.info(f"📊 FastAPI financial data response sent for {company_url} template {template_id} sheet {sheet or 'auto'}")
        
        # Send WebSocket notification for real-time updates
        await websocket_manager.broadcast_to_company({
            "type": "financial_data_updated",
            "company_url": company_url,
            "template_id": template_id,
            "sheet": sheet or "all",
            "periods": periods,
            "timestamp": datetime.now().isoformat(),
            "data_size": len(str(result)) if result else 0
        }, company_url)
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Error in get_financial_data: {e}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

# Bulk update endpoint - mirrors Django's update_bulk API
@fastapi_router.post("/company/{company_url}/financial-data/update-bulk/", tags=["Financial Data"])
async def update_bulk_financial_data(
    request: BulkUpdateRequest,
    company_url: str = Path(..., description="Company URL identifier"),
    template_id: str = Query(..., description="Template/Portfolio ID"),
    current_user: AuthenticatedUser = Depends(get_current_user)
):
    """
    Optimized bulk updates for financial data with minimal database operations.
    Mirrors Django's update_bulk endpoint structure.
    
    Expected payload format:
    {
        "changes": [
            {
                "new_value": 1000.50,
                "rowID": "row_identifier",
                "column_name": "Jan-24",
                "sheet_name": "balance_sheet"
            },
            ...
        ]
    }
    """
    try:
        logger.info(f"🔥 Bulk update request for {company_url}, template {template_id} by user {current_user.email}")
        
        # Validate company access
        if current_user.company_url != company_url:
            logger.warning(f"⚠️ User {current_user.email} attempted to access {company_url} but belongs to {current_user.company_url}")
            raise HTTPException(
                status_code=403,
                detail=f"Access denied: You don't have permission to access {company_url}"
            )
        
        # Validate input
        if not template_id:
            raise HTTPException(
                status_code=400,
                detail="template_id is required as a query parameter"
            )
        
        changes = request.changes
        if not changes:
            raise HTTPException(
                status_code=400,
                detail="changes must be a non-empty list"
            )
        
        # Get tenant configuration
        tenant_config = TenantConfigManager.get_tenant_config(company_url)
        if not tenant_config:
            raise HTTPException(
                status_code=404,
                detail=f"Tenant {company_url} not found"
            )
        
        # Convert Pydantic models to dictionaries for database layer
        changes_dict = [change.dict() for change in changes]
        
        # Execute bulk update
        result = await multi_tenant_db.update_bulk_financial_data(
            company_url=company_url,
            template_id=template_id,
            changes=changes_dict
        )
        
        # Send WebSocket notification for real-time updates if updates were made
        if result.get('rows_updated', 0) > 0:
            websocket_message = {
                "type": "financial_data_updated",
                "company_url": company_url,
                "template_id": str(template_id),
                "sheets_updated": result.get('updated_sheets', []),
                "rows_updated": result.get('rows_updated', 0),
                "timestamp": datetime.now().isoformat(),
                "changes": changes_dict  # Include the original changes for reference
            }
            
            # Broadcast to all connected clients for this company
            logger.info(f"🔔 About to broadcast WebSocket message for {company_url} template {template_id}")
            await websocket_manager.broadcast_to_company(websocket_message, company_url)
            logger.info(f"📡 WebSocket notification sent for {company_url} template {template_id}")
            
            # Add websocket notification status to result
            result['websocket_notified'] = True
        else:
            result['websocket_notified'] = False
        
        logger.info(f"✅ Bulk update completed for {company_url}: {result.get('rows_updated', 0)} rows updated")
        
        return result
        
    except HTTPException:
        raise
    except MultiTenantDatabaseError as e:
        logger.error(f"❌ Database error in bulk update: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"❌ Error in update_bulk_financial_data: {e}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

# Validation endpoint - mirrors Django's validation API (MUST come before direct access routes)
@fastapi_router.get("/company/{company_url}/get_validation_status", tags=["Validation"], response_model=Union[ValidationStatusResponse, Dict[str, List[ValidationError]]])
async def get_validation_status(
    company_url: str = Path(..., description="Company URL identifier"),
    template_id: str = Query(..., description="Template/Portfolio ID"),
    current_user: AuthenticatedUser = Depends(get_current_user)
) -> Union[ValidationStatusResponse, Dict[str, List[ValidationError]]]:
    """
    Get validation status for each data table using tenant configuration.
    Mirrors Django's get_validation_status endpoint.
    
    Returns validation errors for all tables in the tenant configuration.
    Only returns records where is_valid = false.
    """
    try:
        logger.info(f"🔍 Validation status request for {company_url}, template {template_id} by user {current_user.email}")
        
        # Validate company access
        if current_user.company_url != company_url:
            logger.warning(f"⚠️ User {current_user.email} attempted to access {company_url} but belongs to {current_user.company_url}")
            raise HTTPException(
                status_code=403,
                detail=f"Access denied: You don't have permission to access {company_url}"
            )
        
        # Get tenant configuration
        tenant_config = TenantConfigManager.get_tenant_config(company_url)
        if not tenant_config:
            logger.info(f"📋 No validation section exists for client {company_url}")
            return ValidationStatusResponse(
                status="success", 
                message="Validation section does not exist for this client"
            )
        
        if not template_id:
            raise HTTPException(
                status_code=400,
                detail="template_id is required as a query parameter"
            )
        
        result = {}
        schema_name = tenant_config.schema_name
        
        # Process input tables (where validation typically occurs)
        tables_to_validate = tenant_config.input_tables if tenant_config.input_tables else tenant_config.tables
        
        async with multi_tenant_db.pool.acquire() as conn:
            for table in tables_to_validate:
                table_name = table.data_table
                sheet_name = table.sheet_name
                
                try:
                    # Execute dynamic validation query
                    validation_query = f"""
                        SELECT json_agg(result_row) FROM (
                            SELECT 
                                portfolio_id, 
                                validation_rule_name, 
                                is_valid, 
                                error_message,
                                actual_values
                            FROM {schema_name}.get_dynamic_validation_summary($1, $2, $3)
                            WHERE NOT is_valid
                        ) AS result_row
                    """
                    
                    logger.debug(f"Executing validation query for {schema_name}.{table_name}")
                    
                    validation_results = await conn.fetchval(
                        validation_query, 
                        schema_name, 
                        table_name, 
                        template_id
                    )
                    
                    # Handle null results (no validation errors)
                    if validation_results:
                        # Convert to ValidationError objects
                        validation_errors = [
                            ValidationError(**error_data) for error_data in validation_results
                        ]
                        result[sheet_name] = validation_errors
                    else:
                        result[sheet_name] = []
                    
                    logger.debug(f"Validation results for {sheet_name}: {len(result[sheet_name])} errors found")
                    
                except Exception as table_error:
                    logger.error(f"❌ Error validating table {table_name}: {table_error}")
                    # Continue with other tables even if one fails
                    result[sheet_name] = []
        
        logger.info(f"✅ Validation status completed for {company_url}: {sum(len(errors) for errors in result.values())} total errors")
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Error in get_validation_status: {e}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

# Direct access endpoints
@fastapi_router.get("/{company_url}/{sheet_type}/{template_id}", tags=["Direct Access"])
async def get_output_data_direct(
    company_url: str = Path(..., description="Company URL identifier"),
    sheet_type: str = Path(..., description="Sheet type"),
    template_id: str = Path(..., description="Template/Portfolio ID"),
    periods: int = Query(72, description="Number of periods to retrieve"),
    force_refresh: bool = Query(False, description="Force refresh from database")
):
    """
    Direct access to output data
    URL: /api/fastapi/{company_url}/{sheet_type}/{template_id}
    """
    try:
        # Validate tenant and sheet type
        if not TenantConfigManager.validate_tenant_access(company_url, sheet_type):
            supported_types = TenantConfigManager.get_supported_sheet_types(company_url)
            raise HTTPException(
                status_code=400, 
                detail=f"Sheet type {sheet_type} not supported for {company_url}. Supported types: {supported_types}"
            )
        
        # Get the data
        data = await multi_tenant_db.get_output_data(
            company_url=company_url,
            sheet_type=sheet_type,
            template_id=template_id,
            periods=periods,
            force_refresh=force_refresh
        )
        
        if not data:
            raise HTTPException(
                status_code=404, 
                detail=f"No data found for {company_url}.{sheet_type} template {template_id}"
            )
        sheet_data = {
            "metadata": {
                "generated_at": datetime.now().isoformat(),
                "total_sheets": 1,
                "portfolio_id": template_id,
                "schema": tenant_config.schema_name,
                "generator_version": "2.0",
                "request_params": {
                    "sheet_type": sheet_type,
                    "limit_periods": 72,
                    "app_code": "30003",
                    "force_refresh": false
                },
                "api_version": "1.0",
                "endpoint": "universal_output_api",
                "cache_status": "generated"
            },
            "sheets": [data]
        }
        
        return sheet_data
        
    except HTTPException:
        raise
    except MultiTenantDatabaseError as e:
        logger.error(f"❌ Direct access error for {company_url}.{sheet_type}.{template_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        logger.error(f"❌ Unexpected error for {company_url}.{sheet_type}.{template_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")

@fastapi_router.post("/{company_url}/all/{template_id}", tags=["Direct Access"])
async def get_all_output_data(
    request: OutputDataRequest,
    company_url: str = Path(..., description="Company URL identifier"),
    template_id: str = Path(..., description="Template/Portfolio ID")
):
    """
    Get ALL output data for a tenant
    URL: /api/fastapi/{company_url}/all/{template_id}
    """
    try:
        # Get all data
        data = await multi_tenant_db.get_all_output_data(
            company_url=company_url,
            template_id=template_id,
            periods=request.periods,
            sheet_types=request.sheet_types
        )
        
        return data
        
    except HTTPException:
        raise
    except MultiTenantDatabaseError as e:
        logger.error(f"❌ All data error for {company_url}.{template_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        logger.error(f"❌ Unexpected error for {company_url}.{template_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")

# Validation endpoint - mirrors Django's validation API (MUST come before direct access routes)
@fastapi_router.get("/company/{company_url}/get_validation_status", tags=["Validation"], response_model=Union[ValidationStatusResponse, Dict[str, List[ValidationError]]])
async def get_validation_status(
    company_url: str = Path(..., description="Company URL identifier"),
    template_id: str = Query(..., description="Template/Portfolio ID"),
    current_user: AuthenticatedUser = Depends(get_current_user)
) -> Union[ValidationStatusResponse, Dict[str, List[ValidationError]]]:
    """
    Get validation status for each data table using tenant configuration.
    Mirrors Django's get_validation_status endpoint.
    
    Returns validation errors for all tables in the tenant configuration.
    Only returns records where is_valid = false.
    """
    try:
        logger.info(f"🔍 Validation status request for {company_url}, template {template_id} by user {current_user.email}")
        
        # Validate company access
        if current_user.company_url != company_url:
            logger.warning(f"⚠️ User {current_user.email} attempted to access {company_url} but belongs to {current_user.company_url}")
            raise HTTPException(
                status_code=403,
                detail=f"Access denied: You don't have permission to access {company_url}"
            )
        
        # Get tenant configuration
        tenant_config = TenantConfigManager.get_tenant_config(company_url)
        if not tenant_config:
            logger.info(f"📋 No validation section exists for client {company_url}")
            return ValidationStatusResponse(
                status="success", 
                message="Validation section does not exist for this client"
            )
        
        if not template_id:
            raise HTTPException(
                status_code=400,
                detail="template_id is required as a query parameter"
            )
        
        result = {}
        schema_name = tenant_config.schema_name
        
        # Process input tables (where validation typically occurs)
        tables_to_validate = tenant_config.input_tables if tenant_config.input_tables else tenant_config.tables
        
        async with multi_tenant_db.pool.acquire() as conn:
            for table in tables_to_validate:
                table_name = table.data_table
                sheet_name = table.sheet_name
                
                try:
                    # Execute dynamic validation query
                    validation_query = f"""
                        SELECT json_agg(result_row) FROM (
                            SELECT 
                                portfolio_id, 
                                validation_rule_name, 
                                is_valid, 
                                error_message,
                                actual_values
                            FROM {schema_name}.get_dynamic_validation_summary($1, $2, $3)
                            WHERE NOT is_valid
                        ) AS result_row
                    """
                    
                    logger.debug(f"Executing validation query for {schema_name}.{table_name}")
                    
                    validation_results = await conn.fetchval(
                        validation_query, 
                        schema_name, 
                        table_name, 
                        template_id
                    )
                    
                    # Handle null results (no validation errors)
                    if validation_results:
                        # Convert to ValidationError objects
                        validation_errors = [
                            ValidationError(**error_data) for error_data in validation_results
                        ]
                        result[sheet_name] = validation_errors
                    else:
                        result[sheet_name] = []
                    
                    logger.debug(f"Validation results for {sheet_name}: {len(result[sheet_name])} errors found")
                    
                except Exception as table_error:
                    logger.error(f"❌ Error validating table {table_name}: {table_error}")
                    # Continue with other tables even if one fails
                    result[sheet_name] = []
        
        logger.info(f"✅ Validation status completed for {company_url}: {sum(len(errors) for errors in result.values())} total errors")
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Error in get_validation_status: {e}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

# WebSocket endpoints
@app.websocket("/ws/{company_url}")
async def websocket_endpoint(websocket: WebSocket, company_url: str, client_id: str = Query(None)):
    """
    WebSocket endpoint for real-time updates
    URL: ws://localhost:8002/ws/{company_url}?client_id=optional_client_id
    """
    await websocket_manager.connect(websocket, company_url, client_id)
    
    try:
        while True:
            # Receive messages from client
            data = await websocket.receive_text()
            
            try:
                message = json.loads(data)
                message_type = message.get("type", "unknown")
                
                logger.info(f"📨 WebSocket message from {company_url}: {message_type}")
                
                # Handle different message types
                if message_type == "ping":
                    # Update last ping time
                    if websocket in websocket_manager.connection_metadata:
                        websocket_manager.connection_metadata[websocket]["last_ping"] = datetime.now().isoformat()
                    
                    # Send pong response
                    await websocket_manager.send_personal_message({
                        "type": "pong",
                        "timestamp": datetime.now().isoformat()
                    }, websocket)
                
                elif message_type == "subscribe":
                    # Handle subscription to specific data updates
                    subscription_type = message.get("subscription_type", "all")
                    template_id = message.get("template_id")
                    
                    await websocket_manager.send_personal_message({
                        "type": "subscription_confirmed",
                        "subscription_type": subscription_type,
                        "template_id": template_id,
                        "timestamp": datetime.now().isoformat()
                    }, websocket)
                
                elif message_type == "request_data":
                    # Handle real-time data requests
                    sheet_type = message.get("sheet_type")
                    template_id = message.get("template_id")
                    
                    if sheet_type and template_id:
                        try:
                            # Get the requested data
                            data = await multi_tenant_db.get_output_data(
                                company_url=company_url,
                                sheet_type=sheet_type,
                                template_id=template_id,
                                periods=message.get("periods", 72),
                                force_refresh=message.get("force_refresh", False)
                            )
                            
                            # Send data response
                            await websocket_manager.send_personal_message({
                                "type": "data_response",
                                "sheet_type": sheet_type,
                                "template_id": template_id,
                                "data": data,
                                "timestamp": datetime.now().isoformat()
                            }, websocket)
                            
                        except Exception as e:
                            await websocket_manager.send_personal_message({
                                "type": "error",
                                "message": f"Failed to fetch data: {str(e)}",
                                "timestamp": datetime.now().isoformat()
                            }, websocket)
                
                else:
                    # Echo unknown messages
                    await websocket_manager.send_personal_message({
                        "type": "echo",
                        "original_message": message,
                        "timestamp": datetime.now().isoformat()
                    }, websocket)
                    
            except json.JSONDecodeError:
                await websocket_manager.send_personal_message({
                    "type": "error",
                    "message": "Invalid JSON format",
                    "timestamp": datetime.now().isoformat()
                }, websocket)
                
    except WebSocketDisconnect:
        websocket_manager.disconnect(websocket)
    except Exception as e:
        logger.error(f"❌ WebSocket error for {company_url}: {e}")
        websocket_manager.disconnect(websocket)

@app.get("/ws/stats", tags=["WebSocket"])
async def get_websocket_stats():
    """
    Get WebSocket connection statistics
    """
    return websocket_manager.get_connection_stats()

@app.post("/ws/broadcast/{company_url}", tags=["WebSocket"])
async def broadcast_to_company(company_url: str, message: Dict[str, Any]):
    """
    Broadcast a message to all WebSocket connections for a specific company
    """
    await websocket_manager.broadcast_to_company(message, company_url)
    return {"status": "broadcasted", "company_url": company_url, "timestamp": datetime.now().isoformat()}

@app.post("/ws/broadcast/all", tags=["WebSocket"])
async def broadcast_to_all(message: Dict[str, Any]):
    """
    Broadcast a message to all WebSocket connections
    """
    await websocket_manager.broadcast_to_all(message)
    return {"status": "broadcasted", "timestamp": datetime.now().isoformat()}

# Include the router in the main app
app.include_router(fastapi_router)

# Root endpoint
@app.get("/", tags=["Info"])
async def root():
    """
    Root endpoint with service information
    """
    tenants = TenantConfigManager.get_all_tenants()
    total_tables = sum(len(tenant.tables) for tenant in tenants)
    
    return {
        "service": "Multi-Tenant Output Tables Service",
        "version": "2.0.0",
        "description": "High-performance FastAPI service for ALL output tables across ALL tenants",
        "status": "running",
        "timestamp": datetime.now().isoformat(),
        "tenants": {
            "count": len(tenants),
            "total_tables": total_tables,
            "supported_tenants": [tenant.company_url for tenant in tenants]
        },
        "endpoints": {
            "health": "/health",
            "tenants": "/api/fastapi/tenants",
            "output_data": "/api/fastapi/company/{company_url}/output-data/{sheet_type}/",
            "direct_access": "/api/fastapi/{company_url}/{sheet_type}/{template_id}",
            "validation": "/api/fastapi/validate/{company_url}/{template_id}",
            "lookup": "/api/fastapi/lookup/{company_url}/template-id"
        }
    }

@app.get("/api/fastapi/company/{company_url}/output-data/cf/")
async def get_cf_output_data(
    company_url: str,
    start_timestamp: str,
    end_timestamp: str
):
    """Get Cash Flow output data for a specific company and time range"""
    try:
        logger.info(f"🔍 Fetching CF output data for company: {company_url}")
        
        # Get data from multi-tenant database
        data = await multi_tenant_db.get_output_data(
            company_url=company_url,
            table_type="cf",
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp
        )
        
        logger.info(f"✅ Successfully retrieved {len(data)} CF records")
        return data
        
    except Exception as e:
        logger.error(f"❌ Error fetching CF output data: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))



if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8002,  # Same port as the original service
        reload=True,
        log_level="info"
    ) 
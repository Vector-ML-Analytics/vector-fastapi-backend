"""
Multi-Tenant Configuration for FastAPI Output Tables Service
Mirrors Django's tenant configuration system for scalable multi-tenancy
"""

import logging
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)

class ClientType(str, Enum):
    """Client types for different business domains"""
    FINANCIAL = "financial"
    HEALTHCARE = "healthcare"
    ORIGINATION = "origination"
    STATEMENTS = "statements"
    MANAGEMENT = "management"

@dataclass
class TableConfig:
    """Configuration for a specific table"""
    meta_table: str
    data_table: str
    sheet_name: str
    sheet_id: str
    sheet_type: str
    app_code: str = "30003"
    sort_order: int = 0
    is_active: bool = True

@dataclass
class TenantConfig:
    """Complete tenant configuration"""
    company_url: str
    schema_name: str
    display_name: str
    client_type: ClientType
    tables: List[TableConfig]
    input_tables: List[TableConfig]
    features: List[str]
    cache_timeout: int = 3600
    is_active: bool = True

    def __post_init__(self):
        if self.features is None:
            self.features = []

class TenantConfigManager:
    """
    Manages tenant configurations with fallback to hardcoded configs
    Mirrors Django's tenant configuration system
    """
    
    _configs: Dict[str, TenantConfig] = {}
    _initialized = False
    
    @classmethod
    def initialize_configs(cls):
        """Initialize all tenant configurations"""
        if cls._initialized:
            return
        
        # c10020 - Equipment Financing
        cls._configs['c10020'] = TenantConfig(
            company_url='c10020',
            schema_name='db_10020',
            display_name='Equipment Financing',
            client_type=ClientType.FINANCIAL,
            tables=[
                # Output tables (30003 series)
                TableConfig(
                    sheet_type='amort-all',
                    sheet_name='Amortization All',
                    sheet_id='Amort_All',
                    meta_table='30003_Output_Amort_All_Meta',
                    data_table='30003_Output_Amort_All_Data',
                    app_code='30003',
                    is_active=True
                ),
                TableConfig(
                    sheet_type='amort-equipment',
                    sheet_name='Amortization Equipment',
                    sheet_id='Amort_Equipment',
                    meta_table='30003_Output_Amort_Equipment_Meta',
                    data_table='30003_Output_Amort_Equipment_Data',
                    app_code='30003',
                    is_active=True
                ),
                TableConfig(
                    sheet_type='amort-broker',
                    sheet_name='Amortization Broker',
                    sheet_id='Amort_Broker',
                    meta_table='30003_Output_Amort_Broker_Meta',
                    data_table='30003_Output_Amort_Broker_Data',
                    app_code='30003',
                    is_active=True
                ),
                TableConfig(
                    sheet_type='bs',
                    sheet_name='Balance Sheet',
                    sheet_id='bs',
                    meta_table='30003_Output_bs_Meta',
                    data_table='30003_Output_bs_Data',
                    app_code='30003',
                    is_active=True
                ),
                TableConfig(
                    sheet_type='is',
                    sheet_name='Income Statement',
                    sheet_id='is',
                    meta_table='30003_Output_is_Meta',
                    data_table='30003_Output_is_Data',
                    app_code='30003',
                    is_active=True
                ),
                TableConfig(
                    sheet_type='cf',
                    sheet_name='Cash Flow',
                    sheet_id='cf',
                    meta_table='30003_Output_cf_Meta',
                    data_table='30003_Output_cf_Data',
                    app_code='30003',
                    is_active=True
                )
            ],
            input_tables=[
                # Input tables (70003 series) - these are for financial-data endpoint
                TableConfig(
                    sheet_type='balance_sheet',  # Full name for PostgreSQL function
                    sheet_name='Balance Sheet',
                    sheet_id='balance_sheet',
                    meta_table='70003_Input_balance_sheet_Meta',
                    data_table='70003_Input_balance_sheet_Data',
                    app_code='70003',
                    is_active=True
                ),
                TableConfig(
                    sheet_type='income_statement',  # Full name for PostgreSQL function
                    sheet_name='Income Statement',
                    sheet_id='income_statement',
                    meta_table='70003_Input_income_statement_Meta',
                    data_table='70003_Input_income_statement_Data',
                    app_code='70003',
                    is_active=True
                ),
                TableConfig(
                    sheet_type='Broker',  # Matches PostgreSQL function expectation
                    sheet_name='Broker Mix %',
                    sheet_id='broker_mix_testing',
                    meta_table='70003_Input_Broker_Meta',
                    data_table='70003_Input_Broker_Data',
                    app_code='70003',
                    is_active=True
                ),
                TableConfig(
                    sheet_type='Credit',  # Matches PostgreSQL function expectation
                    sheet_name='Credit Risk Analysis',
                    sheet_id='credit_risk_analysis',
                    meta_table='70003_Input_Credit_Meta',
                    data_table='70003_Input_Credit_Data',
                    app_code='70003',
                    is_active=True
                ),
                TableConfig(
                    sheet_type='Deposit',  # Matches PostgreSQL function expectation
                    sheet_name='Deposit Analysis',
                    sheet_id='deposit_analysis',
                    meta_table='70003_Input_Deposit_Meta',
                    data_table='70003_Input_Deposit_Data',
                    app_code='70003',
                    is_active=True
                ),
                TableConfig(
                    sheet_type='Growth',  # Matches PostgreSQL function expectation
                    sheet_name='Growth Metrics',
                    sheet_id='growth_metrics',
                    meta_table='70003_Input_Growth_Meta',
                    data_table='70003_Input_Growth_Data',
                    app_code='70003',
                    is_active=True
                )
            ],
            features=['multi_tenant', 'caching', 'validation'],
            cache_timeout=3600,
            is_active=True
        )
        
        # c10022 - Healthcare Analytics
        cls._configs['c10022'] = TenantConfig(
            company_url='c10022',
            schema_name='db_10022',
            display_name='Healthcare Analytics',
            client_type=ClientType.HEALTHCARE,
            tables=[
                TableConfig(
                    sheet_type='all-output',
                    sheet_name='All Clinics Output',
                    sheet_id='all_clinics',
                    meta_table='30003_Output_All_Meta',
                    data_table='30003_Output_All_Data',
                    app_code='30003',
                    is_active=True
                ),
                TableConfig(
                    sheet_type='clinic1-output',
                    sheet_name='Clinic 1 Output',
                    sheet_id='clinic1',
                    meta_table='30003_Output_Clinic1_Meta',
                    data_table='30003_Output_Clinic1_Data',
                    app_code='30003',
                    is_active=True
                ),
                TableConfig(
                    sheet_type='clinic2-output',
                    sheet_name='Clinic 2 Output',
                    sheet_id='clinic2',
                    meta_table='30003_Output_Clinic2_Meta',
                    data_table='30003_Output_Clinic2_Data',
                    app_code='30003',
                    is_active=True
                ),
                TableConfig(
                    sheet_type='clinic3-output',
                    sheet_name='Clinic 3 Output',
                    sheet_id='clinic3',
                    meta_table='30003_Output_Clinic3_Meta',
                    data_table='30003_Output_Clinic3_Data',
                    app_code='30003',
                    is_active=True
                ),
                TableConfig(
                    sheet_type='clinic4-output',
                    sheet_name='Clinic 4 Output',
                    sheet_id='clinic4',
                    meta_table='30003_Output_Clinic4_Meta',
                    data_table='30003_Output_Clinic4_Data',
                    app_code='30003',
                    is_active=True
                )
            ],
            input_tables=[
                # Healthcare input tables (70003 series)
                TableConfig(
                    sheet_type='Clinic1',  # Matches PostgreSQL function expectation
                    sheet_name='Clinic 1 Analytics',
                    sheet_id='clinic1_analytics',
                    meta_table='70003_Input_Clinic1_Meta',
                    data_table='70003_Input_Clinic1_Data',
                    app_code='70003',
                    is_active=True
                ),
                TableConfig(
                    sheet_type='Clinic2',
                    sheet_name='Clinic 2 Analytics',
                    sheet_id='clinic2_analytics',
                    meta_table='70003_Input_Clinic2_Meta',
                    data_table='70003_Input_Clinic2_Data',
                    app_code='70003',
                    is_active=True
                ),
                TableConfig(
                    sheet_type='Clinic3',
                    sheet_name='Clinic 3 Analytics',
                    sheet_id='clinic3_analytics',
                    meta_table='70003_Input_Clinic3_Meta',
                    data_table='70003_Input_Clinic3_Data',
                    app_code='70003',
                    is_active=True
                ),
                TableConfig(
                    sheet_type='Clinic4',
                    sheet_name='Clinic 4 Analytics',
                    sheet_id='clinic4_analytics',
                    meta_table='70003_Input_Clinic4_Meta',
                    data_table='70003_Input_Clinic4_Data',
                    app_code='70003',
                    is_active=True
                )
            ],
            features=['healthcare_analytics', 'multi_clinic', 'caching'],
            cache_timeout=3600,
            is_active=True
        )
        
        # c10024 - Origination
        cls._configs['c10024'] = TenantConfig(
            company_url='c10024',
            schema_name='db_10024',
            display_name='Origination',
            client_type=ClientType.ORIGINATION,
            tables=[
                TableConfig(
                    sheet_type='growth',
                    sheet_name='Growth Metrics',
                    sheet_id='growth',
                    meta_table='30003_Output_Growth_Meta',
                    data_table='30003_Output_Growth_Data',
                    app_code='30003',
                    is_active=True
                ),
                TableConfig(
                    sheet_type='credit_risk',
                    sheet_name='Credit Risk',
                    sheet_id='credit_risk',
                    meta_table='30003_Output_Credit_Risk_Meta',
                    data_table='30003_Output_Credit_Risk_Data',
                    app_code='30003',
                    is_active=True
                ),
                TableConfig(
                    sheet_type='balance_sheet',
                    sheet_name='Balance Sheet',
                    sheet_id='balance_sheet',
                    meta_table='30003_Output_Balance_Sheet_Meta',
                    data_table='30003_Output_Balance_Sheet_Data',
                    app_code='30003',
                    is_active=True
                ),
                TableConfig(
                    sheet_type='income_statement',
                    sheet_name='Income Statement',
                    sheet_id='income_statement',
                    meta_table='30003_Output_Income_Statement_Meta',
                    data_table='30003_Output_Income_Statement_Data',
                    app_code='30003',
                    is_active=True
                ),
                TableConfig(
                    sheet_type='historical_financial',
                    sheet_name='Historical Financial',
                    sheet_id='historical_financial',
                    meta_table='30003_Output_Historical_Financial_Meta',
                    data_table='30003_Output_Historical_Financial_Data',
                    app_code='30003',
                    is_active=True
                )
            ],
            input_tables=[
                # Origination input tables (70003 series)
                TableConfig(
                    sheet_type='Growth',
                    sheet_name='Growth Metrics',
                    sheet_id='growth',
                    meta_table='70003_Input_Growth_Meta',
                    data_table='70003_Input_Growth_Data',
                    app_code='70003',
                    is_active=True
                ),
                TableConfig(
                    sheet_type='Credit_Risk_Assumptions',
                    sheet_name='Credit Risk Assumptions',
                    sheet_id='credit_risk',
                    meta_table='70003_Input_Credit_Risk_Assumptions_Meta',
                    data_table='70003_Input_Credit_Risk_Assumptions_Data',
                    app_code='70003',
                    is_active=True
                ),
                TableConfig(
                    sheet_type='Balance_Sheet',
                    sheet_name='Balance Sheet',
                    sheet_id='balance_sheet',
                    meta_table='70003_Input_Balance_Sheet_Meta',
                    data_table='70003_Input_Balance_Sheet_Data',
                    app_code='70003',
                    is_active=True
                ),
                TableConfig(
                    sheet_type='Income_sheet',
                    sheet_name='Income Statement',
                    sheet_id='income_statement',
                    meta_table='70003_Input_Income_sheet_Meta',
                    data_table='70003_Input_Income_sheet_Data',
                    app_code='70003',
                    is_active=True
                ),
                TableConfig(
                    sheet_type='Historical_Financial',
                    sheet_name='Historical Financial Data',
                    sheet_id='historical_financial',
                    meta_table='70003_Input_Historical_Financial_Meta',
                    data_table='70003_Input_Historical_Financial_Data',
                    app_code='70003',
                    is_active=True
                )
            ],
            features=['origination_analytics', 'credit_risk', 'caching'],
            cache_timeout=3600,
            is_active=True
        )
        
        # c10025 - Financial Management
        cls._configs['c10025'] = TenantConfig(
            company_url='c10025',
            schema_name='db_10025',
            display_name='Financial Management',
            client_type=ClientType.FINANCIAL,
            tables=[
                TableConfig(
                    sheet_type='financial_management',
                    sheet_name='Financial Management',
                    sheet_id='financial_management',
                    meta_table='30003_Output_Financial_Management_Meta',
                    data_table='30003_Output_Financial_Management_Data',
                    app_code='30003',
                    is_active=True
                ),
                TableConfig(
                    sheet_type='portfolio_analysis',
                    sheet_name='Portfolio Analysis',
                    sheet_id='portfolio_analysis',
                    meta_table='30003_Output_Portfolio_Analysis_Meta',
                    data_table='30003_Output_Portfolio_Analysis_Data',
                    app_code='30003',
                    is_active=True
                ),
                TableConfig(
                    sheet_type='risk_metrics',
                    sheet_name='Risk Metrics',
                    sheet_id='risk_metrics',
                    meta_table='30003_Output_Risk_Metrics_Meta',
                    data_table='30003_Output_Risk_Metrics_Data',
                    app_code='30003',
                    is_active=True
                ),
                TableConfig(
                    sheet_type='performance_tracking',
                    sheet_name='Performance Tracking',
                    sheet_id='performance_tracking',
                    meta_table='30003_Output_Performance_Tracking_Meta',
                    data_table='30003_Output_Performance_Tracking_Data',
                    app_code='30003',
                    is_active=True
                ),
                TableConfig(
                    sheet_type='compliance_reporting',
                    sheet_name='Compliance Reporting',
                    sheet_id='compliance_reporting',
                    meta_table='30003_Output_Compliance_Reporting_Meta',
                    data_table='30003_Output_Compliance_Reporting_Data',
                    app_code='30003',
                    is_active=True
                )
            ],
            input_tables=[
                # Financial Management input tables (70003 series)
                TableConfig(
                    sheet_type='Financial_Management',
                    sheet_name='Financial Management',
                    sheet_id='financial_management',
                    meta_table='70003_Input_Financial_Management_Meta',
                    data_table='70003_Input_Financial_Management_Data',
                    app_code='70003',
                    is_active=True
                ),
                TableConfig(
                    sheet_type='Portfolio_Analysis',
                    sheet_name='Portfolio Analysis',
                    sheet_id='portfolio_analysis',
                    meta_table='70003_Input_Portfolio_Analysis_Meta',
                    data_table='70003_Input_Portfolio_Analysis_Data',
                    app_code='70003',
                    is_active=True
                ),
                TableConfig(
                    sheet_type='Risk_Metrics',
                    sheet_name='Risk Metrics',
                    sheet_id='risk_metrics',
                    meta_table='70003_Input_Risk_Metrics_Meta',
                    data_table='70003_Input_Risk_Metrics_Data',
                    app_code='70003',
                    is_active=True
                ),
                TableConfig(
                    sheet_type='Performance_Tracking',
                    sheet_name='Performance Tracking',
                    sheet_id='performance_tracking',
                    meta_table='70003_Input_Performance_Tracking_Meta',
                    data_table='70003_Input_Performance_Tracking_Data',
                    app_code='70003',
                    is_active=True
                ),
                TableConfig(
                    sheet_type='Compliance_Reporting',
                    sheet_name='Compliance Reporting',
                    sheet_id='compliance_reporting',
                    meta_table='70003_Input_Compliance_Reporting_Meta',
                    data_table='70003_Input_Compliance_Reporting_Data',
                    app_code='70003',
                    is_active=True
                )
            ],
            features=['financial_management', 'portfolio_analysis', 'caching'],
            cache_timeout=3600,
            is_active=True
        )
        
        # c10026 - Equipment Financing C10026
        cls._configs['c10026'] = TenantConfig(
            company_url='c10026',
            schema_name='db_10026',
            display_name='Equipment Financing C10026',
            client_type=ClientType.FINANCIAL,
            tables=[
                # Output tables (30003 series) - same as c10020
                TableConfig(
                    sheet_type='amort-all',
                    sheet_name='Amortization All',
                    sheet_id='amort_all',
                    meta_table='30003_Output_Amort_All_Meta',
                    data_table='30003_Output_Amort_All_Data',
                    app_code='30003',
                    is_active=True
                ),
                TableConfig(
                    sheet_type='amort-equipment',
                    sheet_name='Amortization Equipment',
                    sheet_id='amort_equipment',
                    meta_table='30003_Output_Amort_Equipment_Meta',
                    data_table='30003_Output_Amort_Equipment_Data',
                    app_code='30003',
                    is_active=True
                ),
                TableConfig(
                    sheet_type='amort-broker',
                    sheet_name='Amortization Broker',
                    sheet_id='amort_broker',
                    meta_table='30003_Output_Amort_Broker_Meta',
                    data_table='30003_Output_Amort_Broker_Data',
                    app_code='30003',
                    is_active=True
                ),
                TableConfig(
                    sheet_type='bs',
                    sheet_name='Balance Sheet',
                    sheet_id='balance_sheet',
                    meta_table='30003_Output_bs_Meta',
                    data_table='30003_Output_bs_Data',
                    app_code='30003',
                    is_active=True
                ),
                TableConfig(
                    sheet_type='is',
                    sheet_name='Income Statement',
                    sheet_id='income_statement',
                    meta_table='30003_Output_is_Meta',
                    data_table='30003_Output_is_Data',
                    app_code='30003',
                    is_active=True
                ),
                TableConfig(
                    sheet_type='cf',
                    sheet_name='Cash Flow',
                    sheet_id='cash_flow',
                    meta_table='30003_Output_cf_Meta',
                    data_table='30003_Output_cf_Data',
                    app_code='30003',
                    is_active=True
                )
            ],
            input_tables=[
                # Input tables (70003 series) - same as c10020
                TableConfig(
                    sheet_type='balance_sheet',
                    sheet_name='Balance Sheet',
                    sheet_id='balance_sheet',
                    meta_table='70003_Input_balance_sheet_Meta',
                    data_table='70003_Input_balance_sheet_Data',
                    app_code='70003',
                    is_active=True
                ),
                TableConfig(
                    sheet_type='income_statement',
                    sheet_name='Income Statement',
                    sheet_id='income_statement',
                    meta_table='70003_Input_income_statement_Meta',
                    data_table='70003_Input_income_statement_Data',
                    app_code='70003',
                    is_active=True
                ),
                TableConfig(
                    sheet_type='Broker',
                    sheet_name='Broker Mix %',
                    sheet_id='broker_mix_testing',
                    meta_table='70003_Input_Broker_Meta',
                    data_table='70003_Input_Broker_Data',
                    app_code='70003',
                    is_active=True
                ),
                TableConfig(
                    sheet_type='Credit',
                    sheet_name='Credit Risk Analysis',
                    sheet_id='credit_risk_analysis',
                    meta_table='70003_Input_Credit_Meta',
                    data_table='70003_Input_Credit_Data',
                    app_code='70003',
                    is_active=True
                ),
                TableConfig(
                    sheet_type='Deposit',
                    sheet_name='Deposit Analysis',
                    sheet_id='deposit_analysis',
                    meta_table='70003_Input_Deposit_Meta',
                    data_table='70003_Input_Deposit_Data',
                    app_code='70003',
                    is_active=True
                ),
                TableConfig(
                    sheet_type='Growth',
                    sheet_name='Growth Metrics',
                    sheet_id='growth_metrics',
                    meta_table='70003_Input_Growth_Meta',
                    data_table='70003_Input_Growth_Data',
                    app_code='70003',
                    is_active=True
                )
            ],
            features=['multi_tenant', 'caching', 'validation'],
            cache_timeout=3600,
            is_active=True
        )
        
        cls._initialized = True
        logger.info("✅ Tenant configurations initialized successfully")

    @classmethod
    def get_tenant_config(cls, company_url: str) -> Optional[TenantConfig]:
        """Get tenant configuration by company URL"""
        cls.initialize_configs()
        config = cls._configs.get(company_url)
        if not config:
            logger.warning(f"No tenant configuration found for {company_url}")
            return None
        
        if not config.is_active:
            logger.warning(f"Tenant {company_url} is not active")
            return None
        
        logger.debug(f"Retrieved tenant config for {company_url}: {config.display_name}")
        return config
    
    @classmethod
    def get_all_tenants(cls) -> List[TenantConfig]:
        """Get all active tenant configurations"""
        cls.initialize_configs()
        return [config for config in cls._configs.values() if config.is_active]
    
    @classmethod
    def get_tenant_by_schema(cls, schema_name: str) -> Optional[TenantConfig]:
        """Get tenant configuration by schema name"""
        cls.initialize_configs()
        for config in cls._configs.values():
            if config.schema_name == schema_name:
                return config
        return None
    
    @classmethod
    def get_table_config(cls, company_url: str, sheet_type: str) -> Optional[TableConfig]:
        """Get specific table configuration for a tenant"""
        cls.initialize_configs()
        config = cls._configs.get(company_url)
        if not config:
            return None
        
        for table in config.tables:
            if table.sheet_type == sheet_type:
                return table
        
        logger.warning(f"No table config found for {company_url}.{sheet_type}")
        return None
    
    @classmethod
    def get_supported_sheet_types(cls, company_url: str) -> List[str]:
        """Get all supported sheet types for a tenant"""
        cls.initialize_configs()
        config = cls._configs.get(company_url)
        if not config:
            return []
        
        return [table.sheet_type for table in config.tables if table.is_active]
    
    @classmethod
    def validate_tenant_access(cls, company_url: str, sheet_type: str) -> bool:
        """Validate if a tenant has access to a specific sheet type"""
        return sheet_type in cls.get_supported_sheet_types(company_url)
    
    @classmethod
    def get_tenant_features(cls, company_url: str) -> List[str]:
        """Get enabled features for a tenant"""
        cls.initialize_configs()
        config = cls._configs.get(company_url)
        return config.features if config else []
    
    @classmethod
    def has_feature(cls, company_url: str, feature: str) -> bool:
        """Check if a tenant has a specific feature enabled"""
        return feature in cls.get_tenant_features(company_url)
    
    @classmethod
    def get_input_sheet_types(cls, company_url: str) -> List[str]:
        """Get supported input sheet types for a tenant"""
        cls.initialize_configs()
        config = cls._configs.get(company_url)
        if not config:
            return []
        return [table.sheet_type for table in config.input_tables if table.is_active]
    
    @classmethod
    def get_input_table_config(cls, company_url: str, sheet_type: str) -> Optional[TableConfig]:
        """Get input table configuration for a specific sheet type"""
        cls.initialize_configs()
        config = cls._configs.get(company_url)
        if not config:
            return None
        
        for table in config.input_tables:
            if table.sheet_type == sheet_type and table.is_active:
                return table
        return None 
import os
from dotenv import load_dotenv
from typing import Optional

# Load environment variables from .env file
load_dotenv()

class DatabaseConfig:
    """Database configuration that matches Django settings"""
    
    def __init__(self):
        self.database_name = os.getenv('DATABASE_NAME', 'vector_db')
        self.database_user = os.getenv('DATABASE_USER', 'postgres')
        self.database_password = os.getenv('DATABASE_PASSWORD')
        self.database_host = os.getenv('DATABASE_HOST', 'localhost')
        self.database_port = int(os.getenv('DATABASE_PORT', '5432'))
        
        # Validate required settings
        if not self.database_password:
            raise ValueError("DATABASE_PASSWORD environment variable is required")
    
    @property
    def database_url(self) -> str:
        """Get PostgreSQL connection URL"""
        return (
            f"postgresql://{self.database_user}:{self.database_password}"
            f"@{self.database_host}:{self.database_port}/{self.database_name}"
        )
    
    def get_connection_params(self) -> dict:
        """Get connection parameters for asyncpg"""
        return {
            'host': self.database_host,
            'port': self.database_port,
            'user': self.database_user,
            'password': self.database_password,
            'database': self.database_name,
            'server_settings': {
                'application_name': 'fastapi_c10020_amort_service',
                'search_path': 'public'
            }
        }

class FastAPIConfig:
    """FastAPI service configuration"""
    
    def __init__(self):
        self.host = os.getenv('FASTAPI_HOST', '0.0.0.0')
        self.port = int(os.getenv('FASTAPI_PORT', '8001'))
        self.reload = os.getenv('FASTAPI_RELOAD', 'True').lower() == 'true'
        self.log_level = os.getenv('LOG_LEVEL', 'INFO').lower()

class C10020Config:
    """C10020-specific configuration"""
    
    def __init__(self):
        # Database schema and function settings
        self.schema_name = 'db_10020'
        self.sheet_type = 'Amort_All'
        self.app_code = '30003'  # Historical Monthly FPA
        self.default_periods = 72
        
        # PostgreSQL function name
        self.pg_function = 'public.generate_ag_grid_universal_output'
        
        # Connection pool settings
        self.pool_min_size = 5
        self.pool_max_size = 20
        self.pool_timeout = 30

# Global configuration instances
db_config = DatabaseConfig()
fastapi_config = FastAPIConfig()
c10020_config = C10020Config()

def validate_environment():
    """Validate that all required environment variables are set"""
    required_vars = [
        'DATABASE_NAME',
        'DATABASE_USER', 
        'DATABASE_PASSWORD',
        'DATABASE_HOST'
    ]
    
    missing_vars = []
    for var in required_vars:
        if not os.getenv(var):
            missing_vars.append(var)
    
    if missing_vars:
        raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")
    
    print("✅ Environment configuration validated successfully")
    return True 
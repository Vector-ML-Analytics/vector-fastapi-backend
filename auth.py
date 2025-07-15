"""
JWT Authentication for FastAPI
Validates JWT tokens created by Django using django-rest-framework-simplejwt
"""

import jwt
import os
from datetime import datetime
from typing import Optional, Dict, Any
from fastapi import HTTPException, Depends, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel
import asyncpg
import logging

logger = logging.getLogger(__name__)

# JWT Configuration - matches Django settings
JWT_SECRET_KEY = os.getenv("SECRET_KEY")  # Same as Django's SECRET_KEY
JWT_ALGORITHM = "HS256"  # Default algorithm for django-rest-framework-simplejwt

# Security scheme for extracting Bearer tokens
security = HTTPBearer()

class AuthenticatedUser(BaseModel):
    """User model representing authenticated user from JWT token"""
    user_id: int
    email: str
    username: str
    first_name: str
    last_name: str
    company_id: Optional[int] = None
    company_url: Optional[str] = None
    company_name: Optional[str] = None
    job_name: Optional[str] = None
    is_active: bool = True

class JWTValidationError(Exception):
    """Custom exception for JWT validation errors"""
    def __init__(self, message: str, status_code: int = status.HTTP_401_UNAUTHORIZED):
        self.message = message
        self.status_code = status_code
        super().__init__(self.message)

async def get_user_from_database(user_id: int) -> Optional[Dict[str, Any]]:
    """
    Fetch user details from database including company information
    """
    try:
        # Use the same database connection as the multi-tenant system
        from multi_tenant_database import multi_tenant_db
        
        if not multi_tenant_db.connection_established:
            await multi_tenant_db.init_pool()
        
        async with multi_tenant_db.pool.acquire() as conn:
            # Query user with company information
            query = """
                SELECT 
                    u.id,
                    u.email,
                    u.username,
                    u.first_name,
                    u.last_name,
                    u.job_name,
                    u.is_active,
                    c.id as company_id,
                    c.company_url,
                    c.company_name
                FROM auth_app_customuser u
                LEFT JOIN company_company c ON u.company_id = c.id
                WHERE u.id = $1 AND u.is_active = true
            """
            
            result = await conn.fetchrow(query, user_id)
            
            if not result:
                return None
            
            return {
                "user_id": result["id"],
                "email": result["email"],
                "username": result["username"],
                "first_name": result["first_name"] or "",
                "last_name": result["last_name"] or "",
                "job_name": result["job_name"],
                "is_active": result["is_active"],
                "company_id": result["company_id"],
                "company_url": result["company_url"],
                "company_name": result["company_name"]
            }
            
    except Exception as e:
        logger.error(f"❌ Error fetching user {user_id} from database: {e}")
        return None

def decode_jwt_token(token: str) -> Dict[str, Any]:
    """
    Decode and validate JWT token using the same method as Django
    """
    try:
        if not JWT_SECRET_KEY:
            raise JWTValidationError("JWT secret key not configured")
        
        # Decode token using same secret and algorithm as Django
        payload = jwt.decode(
            token,
            JWT_SECRET_KEY,
            algorithms=[JWT_ALGORITHM]
        )
        
        # Validate required claims
        if "user_id" not in payload:
            raise JWTValidationError("Invalid token: missing user_id")
        
        if "exp" not in payload:
            raise JWTValidationError("Invalid token: missing expiration")
        
        # Check if token is expired (jwt.decode already does this, but let's be explicit)
        exp_timestamp = payload["exp"]
        if datetime.utcnow().timestamp() > exp_timestamp:
            raise JWTValidationError("Token has expired")
        
        return payload
        
    except jwt.ExpiredSignatureError:
        raise JWTValidationError("Token has expired")
    except jwt.InvalidTokenError as e:
        raise JWTValidationError(f"Invalid token: {str(e)}")
    except Exception as e:
        logger.error(f"❌ JWT decode error: {e}")
        raise JWTValidationError("Token validation failed")

async def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)) -> AuthenticatedUser:
    """
    FastAPI dependency to get current authenticated user from JWT token
    """
    try:
        # Extract token from credentials
        token = credentials.credentials
        
        # Decode and validate JWT token
        payload = decode_jwt_token(token)
        user_id = payload["user_id"]
        
        # Fetch user details from database
        user_data = await get_user_from_database(user_id)
        
        if not user_data:
            raise JWTValidationError("User not found or inactive")
        
        # Create authenticated user object
        user = AuthenticatedUser(**user_data)
        
        logger.info(f"✅ Authenticated user: {user.email} (company: {user.company_url})")
        return user
        
    except JWTValidationError:
        raise
    except Exception as e:
        logger.error(f"❌ Authentication error: {e}")
        raise JWTValidationError("Authentication failed")

async def get_current_user_optional(credentials: Optional[HTTPAuthorizationCredentials] = Depends(HTTPBearer(auto_error=False))) -> Optional[AuthenticatedUser]:
    """
    Optional authentication dependency - returns None if no token provided
    """
    if not credentials:
        return None
    
    try:
        return await get_current_user(credentials)
    except JWTValidationError:
        return None

def validate_company_access(user: AuthenticatedUser, company_url: str) -> bool:
    """
    Validate that the authenticated user has access to the specified company
    """
    if not user.company_url:
        logger.warning(f"⚠️ User {user.email} has no company assigned")
        return False
    
    if user.company_url != company_url:
        logger.warning(f"⚠️ User {user.email} attempted to access {company_url} but belongs to {user.company_url}")
        return False
    
    return True

def require_company_access(company_url: str):
    """
    Decorator factory to create a dependency that validates company access
    """
    async def validate_access(
        current_user: AuthenticatedUser = Depends(get_current_user)
    ) -> AuthenticatedUser:
        if not validate_company_access(current_user, company_url):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Access denied: You don't have permission to access {company_url}"
            )
        return current_user
    
    return validate_access

# Convenience function for routes that need both auth and company validation
async def get_authenticated_user_for_company(
    company_url: str,
    current_user: AuthenticatedUser = Depends(get_current_user)
) -> AuthenticatedUser:
    """
    Get authenticated user and validate company access in one step
    """
    if not validate_company_access(current_user, company_url):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Access denied: You don't have permission to access {company_url}"
        )
    return current_user 
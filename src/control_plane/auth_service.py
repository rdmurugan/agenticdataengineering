"""
Authentication and Authorization Service for Multi-tenant SaaS
"""

import jwt
import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from passlib.context import CryptContext
from passlib.hash import bcrypt
import secrets
import logging
from enum import Enum

logger = logging.getLogger(__name__)

class UserRole(Enum):
    """User roles within a tenant"""
    VIEWER = "viewer"
    ANALYST = "analyst" 
    ADMIN = "admin"
    TENANT_ADMIN = "tenant_admin"
    PLATFORM_ADMIN = "platform_admin"

@dataclass
class TokenData:
    """JWT token payload data"""
    user_id: str
    tenant_id: str
    role: UserRole
    email: str
    is_admin: bool
    permissions: List[str]
    exp: datetime

@dataclass
class User:
    """User account information"""
    user_id: str
    email: str
    tenant_id: str
    role: UserRole
    hashed_password: str
    is_active: bool
    created_at: datetime
    last_login: Optional[datetime]
    permissions: List[str]

class AuthService:
    """Handles authentication, authorization, and JWT token management"""
    
    def __init__(self, secret_key: Optional[str] = None):
        self.secret_key = secret_key or secrets.token_urlsafe(32)
        self.algorithm = "HS256"
        self.access_token_expire_minutes = 60
        self.refresh_token_expire_days = 7
        
        # Password hashing
        self.pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
        
        # In-memory user store (replace with database in production)
        self.users: Dict[str, User] = {}
        
        # Permission system
        self.role_permissions = {
            UserRole.VIEWER: [
                "read:pipelines",
                "read:quality_reports",
                "read:usage"
            ],
            UserRole.ANALYST: [
                "read:pipelines",
                "read:quality_reports", 
                "read:usage",
                "update:quality_thresholds",
                "create:quality_rules"
            ],
            UserRole.ADMIN: [
                "read:pipelines",
                "read:quality_reports",
                "read:usage", 
                "create:pipelines",
                "update:pipelines",
                "delete:pipelines",
                "update:quality_thresholds",
                "create:quality_rules",
                "manage:notifications"
            ],
            UserRole.TENANT_ADMIN: [
                "*"  # All tenant-level permissions
            ],
            UserRole.PLATFORM_ADMIN: [
                "*",  # All permissions
                "manage:tenants",
                "read:platform_analytics",
                "manage:billing"
            ]
        }
        
        # Initialize with default admin user
        self._create_default_admin()
    
    def _create_default_admin(self):
        """Create default platform admin user"""
        admin_user = User(
            user_id="platform_admin_001",
            email="admin@agenticdata.com",
            tenant_id="platform",
            role=UserRole.PLATFORM_ADMIN,
            hashed_password=self.get_password_hash("admin123!"),
            is_active=True,
            created_at=datetime.utcnow(),
            last_login=None,
            permissions=self.role_permissions[UserRole.PLATFORM_ADMIN]
        )
        self.users[admin_user.user_id] = admin_user
        logger.info("Created default platform admin user")
    
    def get_password_hash(self, password: str) -> str:
        """Hash a password"""
        return self.pwd_context.hash(password)
    
    def verify_password(self, plain_password: str, hashed_password: str) -> bool:
        """Verify a password against its hash"""
        return self.pwd_context.verify(plain_password, hashed_password)
    
    def create_access_token(self, user: User) -> str:
        """Create JWT access token"""
        expire = datetime.utcnow() + timedelta(minutes=self.access_token_expire_minutes)
        
        payload = {
            "sub": user.user_id,
            "tenant_id": user.tenant_id,
            "role": user.role.value,
            "email": user.email,
            "is_admin": user.role in [UserRole.TENANT_ADMIN, UserRole.PLATFORM_ADMIN],
            "permissions": user.permissions,
            "exp": expire,
            "iat": datetime.utcnow(),
            "type": "access"
        }
        
        return jwt.encode(payload, self.secret_key, algorithm=self.algorithm)
    
    def create_refresh_token(self, user: User) -> str:
        """Create JWT refresh token"""
        expire = datetime.utcnow() + timedelta(days=self.refresh_token_expire_days)
        
        payload = {
            "sub": user.user_id,
            "exp": expire,
            "iat": datetime.utcnow(),
            "type": "refresh"
        }
        
        return jwt.encode(payload, self.secret_key, algorithm=self.algorithm)
    
    async def verify_token(self, token: str) -> TokenData:
        """Verify and decode JWT token"""
        try:
            payload = jwt.decode(token, self.secret_key, algorithms=[self.algorithm])
            
            if payload.get("type") != "access":
                raise jwt.InvalidTokenError("Invalid token type")
            
            user_id = payload.get("sub")
            if not user_id:
                raise jwt.InvalidTokenError("Missing user ID")
            
            # Check if user exists and is active
            user = self.users.get(user_id)
            if not user or not user.is_active:
                raise jwt.InvalidTokenError("User not found or inactive")
            
            token_data = TokenData(
                user_id=user_id,
                tenant_id=payload.get("tenant_id"),
                role=UserRole(payload.get("role")),
                email=payload.get("email"),
                is_admin=payload.get("is_admin", False),
                permissions=payload.get("permissions", []),
                exp=datetime.fromtimestamp(payload.get("exp"))
            )
            
            return token_data
            
        except jwt.ExpiredSignatureError:
            raise jwt.InvalidTokenError("Token has expired")
        except jwt.InvalidTokenError as e:
            raise jwt.InvalidTokenError(f"Invalid token: {str(e)}")
    
    async def authenticate_user(self, email: str, password: str) -> Optional[User]:
        """Authenticate user with email and password"""
        # Find user by email
        user = None
        for u in self.users.values():
            if u.email == email:
                user = u
                break
        
        if not user:
            return None
        
        if not user.is_active:
            return None
        
        if not self.verify_password(password, user.hashed_password):
            return None
        
        # Update last login
        user.last_login = datetime.utcnow()
        
        return user
    
    async def create_user(self, 
                         email: str,
                         password: str, 
                         tenant_id: str,
                         role: UserRole = UserRole.VIEWER) -> User:
        """Create a new user"""
        
        # Check if user already exists
        for user in self.users.values():
            if user.email == email:
                raise ValueError(f"User with email {email} already exists")
        
        user_id = f"user_{secrets.token_hex(8)}"
        
        # Get permissions for role
        permissions = self.role_permissions.get(role, [])
        
        user = User(
            user_id=user_id,
            email=email,
            tenant_id=tenant_id,
            role=role,
            hashed_password=self.get_password_hash(password),
            is_active=True,
            created_at=datetime.utcnow(),
            last_login=None,
            permissions=permissions
        )
        
        self.users[user_id] = user
        logger.info(f"Created user {email} for tenant {tenant_id}")
        
        return user
    
    async def update_user_role(self, user_id: str, new_role: UserRole) -> User:
        """Update user role and permissions"""
        if user_id not in self.users:
            raise ValueError(f"User {user_id} not found")
        
        user = self.users[user_id]
        user.role = new_role
        user.permissions = self.role_permissions.get(new_role, [])
        
        logger.info(f"Updated user {user.email} role to {new_role.value}")
        return user
    
    async def deactivate_user(self, user_id: str):
        """Deactivate user account"""
        if user_id not in self.users:
            raise ValueError(f"User {user_id} not found")
        
        user = self.users[user_id]
        user.is_active = False
        
        logger.info(f"Deactivated user {user.email}")
    
    def check_permission(self, token_data: TokenData, required_permission: str) -> bool:
        """Check if user has required permission"""
        # Platform admins have all permissions
        if token_data.role == UserRole.PLATFORM_ADMIN:
            return True
        
        # Tenant admins have all tenant-level permissions
        if token_data.role == UserRole.TENANT_ADMIN and not required_permission.startswith("manage:tenants"):
            return True
        
        # Check specific permissions
        return required_permission in token_data.permissions or "*" in token_data.permissions
    
    async def refresh_access_token(self, refresh_token: str) -> str:
        """Create new access token from refresh token"""
        try:
            payload = jwt.decode(refresh_token, self.secret_key, algorithms=[self.algorithm])
            
            if payload.get("type") != "refresh":
                raise jwt.InvalidTokenError("Invalid token type")
            
            user_id = payload.get("sub")
            user = self.users.get(user_id)
            
            if not user or not user.is_active:
                raise jwt.InvalidTokenError("User not found or inactive")
            
            # Create new access token
            return self.create_access_token(user)
            
        except jwt.ExpiredSignatureError:
            raise jwt.InvalidTokenError("Refresh token has expired")
        except jwt.InvalidTokenError as e:
            raise jwt.InvalidTokenError(f"Invalid refresh token: {str(e)}")
    
    def get_user_by_id(self, user_id: str) -> Optional[User]:
        """Get user by ID"""
        return self.users.get(user_id)
    
    def list_users_by_tenant(self, tenant_id: str) -> List[User]:
        """List all users for a tenant"""
        return [user for user in self.users.values() if user.tenant_id == tenant_id]
    
    async def reset_password(self, user_id: str, new_password: str):
        """Reset user password"""
        if user_id not in self.users:
            raise ValueError(f"User {user_id} not found")
        
        user = self.users[user_id]
        user.hashed_password = self.get_password_hash(new_password)
        
        logger.info(f"Password reset for user {user.email}")
    
    async def change_password(self, user_id: str, current_password: str, new_password: str):
        """Change user password with current password verification"""
        if user_id not in self.users:
            raise ValueError(f"User {user_id} not found")
        
        user = self.users[user_id]
        
        if not self.verify_password(current_password, user.hashed_password):
            raise ValueError("Current password is incorrect")
        
        user.hashed_password = self.get_password_hash(new_password)
        
        logger.info(f"Password changed for user {user.email}")

# Permission decorator for API endpoints
def require_permission(permission: str):
    """Decorator to check permissions on API endpoints"""
    def decorator(func):
        async def wrapper(*args, **kwargs):
            # Extract token_data from kwargs (should be injected by auth dependency)
            token_data = kwargs.get('current_user') or kwargs.get('current_tenant')
            if not token_data:
                raise Exception("Authentication required")
            
            auth_service = AuthService()  # In production, use dependency injection
            if not auth_service.check_permission(token_data, permission):
                raise Exception(f"Permission denied: {permission} required")
            
            return await func(*args, **kwargs)
        return wrapper
    return decorator
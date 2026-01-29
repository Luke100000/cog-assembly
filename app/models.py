"""Database models for users and services."""

import uuid
from sqlalchemy import Column, String
from app.database import Base


class UserModel(Base):
    """User model for authentication and access control."""

    __tablename__ = "users"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    username = Column(String, unique=True, nullable=False, index=True)
    password_hash = Column(String, nullable=False)
    token = Column(
        String, unique=True, nullable=False, default=lambda: uuid.uuid4().hex
    )
    groups = Column(String, default="")

    def __repr__(self):
        return f"<User(username='{self.username}', groups='{self.groups}')>"

    def has_group(self, group: str) -> bool:
        """Check if user has a specific permission group."""
        user_groups = self.get_groups_list()
        return "admin" in user_groups or group in user_groups

    def get_groups_list(self) -> list[str]:
        """Get groups as a list."""
        return [g.strip() for g in self.groups.split(",") if g.strip()]

"""Database models for users and services."""

import uuid
from sqlalchemy import Column, String, Integer, Boolean
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


class ServiceModel(Base):
    """Service model to store service configuration."""

    __tablename__ = "services"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    name = Column(String, unique=True, nullable=False, index=True)
    image = Column(String, nullable=False)
    max_vram = Column(String, nullable=True)
    max_ram = Column(String, nullable=True)
    use_gpu = Column(Boolean, default=True)
    use_cpu = Column(Boolean, default=True)
    max_boot_time = Column(Integer, default=60)
    idle_timeout = Column(Integer, default=3600)
    health_check_type = Column(String, default="none")
    health_check_url = Column(String, default="")
    health_check_regex = Column(String, default="")
    port = Column(Integer, default=8000)
    mounts = Column(String, default="")
    environment = Column(String, default="")
    cpuset_cpus = Column(String, nullable=True)
    permission_group = Column(String, default="")

    def __repr__(self):
        return f"<Service(name='{self.name}', image='{self.image}')>"

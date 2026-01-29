"""Database configuration and session management."""

import uuid
from loguru import logger
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base

# SQLite database path
DATABASE_URL = "sqlite:///./data/users.db"

# Create engine
engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False},  # Needed for SQLite
    echo=False,
)

# Create session factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Create base class for models
Base = declarative_base()


def init_db():
    """Initialize the database by creating all tables."""
    Base.metadata.create_all(bind=engine)


def create_default_user_if_empty():
    """Create a default admin user if the database is empty."""
    from app.models import UserModel
    from app.password import hash_password

    session = SessionLocal()
    try:
        # Check if any users exist
        user_count = session.query(UserModel).count()

        if user_count == 0:
            # Create default admin user
            default_user = UserModel(
                username="admin",
                password_hash=hash_password("admin"),
                token=uuid.uuid4().hex,
                groups="admin",
            )
            session.add(default_user)
            session.commit()

            logger.info("=" * 60)
            logger.info("Default admin user created:")
            logger.info("  Username: admin")
            logger.info("  Password: admin")
            logger.info(f"  Groups: {default_user.groups}")
            logger.info(f"  API Token: {default_user.token}")
            logger.info("  PLEASE CHANGE THE PASSWORD AFTER FIRST LOGIN!")
            logger.info("=" * 60)
    except Exception as e:
        session.rollback()
        logger.error(f"Error creating default user: {e}")
    finally:
        session.close()


def get_session():
    """Get a database session."""
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()

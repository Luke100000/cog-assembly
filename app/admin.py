"""SQLAdmin setup for user management."""

from sqladmin import Admin, ModelView
from sqladmin.authentication import AuthenticationBackend
from starlette.requests import Request
from wtforms.widgets import TextArea

from app.database import engine, SessionLocal
from app.models import UserModel, ServiceModel
from app.password import verify_password


class UserAdmin(ModelView, model=UserModel):
    """Admin view for User management."""

    column_list = [
        UserModel.username,
        UserModel.groups,
    ]
    column_searchable_list = [UserModel.username, UserModel.token]
    column_sortable_list = [
        UserModel.username,
    ]
    column_default_sort = [(UserModel.username, False)]
    form_columns = [
        UserModel.username,
        UserModel.password_hash,
        UserModel.token,
        UserModel.groups,
    ]
    column_details_list = [
        UserModel.id,
        UserModel.username,
        UserModel.token,
        UserModel.groups,
    ]
    name = "User"
    name_plural = "Users"
    icon = "fa-solid fa-user"

    form_args = {
        "groups": {
            "label": "Permission Groups",
            "description": "Enter groups separated by commas (e.g., admin, user, premium). Common groups: admin, stats, user, premium, internal",
            "render_kw": {
                "placeholder": "admin, user, stats",
            },
        },
        "password_hash": {
            "label": "Password Hash",
            "description": "Password hash - edit in admin panel to set a new password",
        },
        "token": {
            "label": "API Token",
            "description": "Unique token for API authentication",
        },
    }


class ServiceAdmin(ModelView, model=ServiceModel):
    """Admin view for Service management."""

    column_list = [
        ServiceModel.name,
        ServiceModel.image,
        ServiceModel.permission_group,
    ]
    column_searchable_list = [ServiceModel.name, ServiceModel.image]
    column_sortable_list = [ServiceModel.name]
    form_columns = [
        ServiceModel.name,
        ServiceModel.image,
        ServiceModel.max_vram,
        ServiceModel.max_ram,
        ServiceModel.use_gpu,
        ServiceModel.use_cpu,
        ServiceModel.max_boot_time,
        ServiceModel.idle_timeout,
        ServiceModel.health_check_type,
        ServiceModel.health_check_url,
        ServiceModel.health_check_regex,
        ServiceModel.port,
        ServiceModel.mounts,
        ServiceModel.environment,
        ServiceModel.cpuset_cpus,
        ServiceModel.permission_group,
    ]
    form_args = {
        "environment": {
            "label": "Environment Variables",
            "description": "Enter one key=value pair per line.",
            "render_kw": {"rows": 4, "style": "font-family:monospace;"},
            "widget": TextArea(),
        },
        "mounts": {
            "label": "Mounts",
            "description": "Enter one mount descriptor per line.",
            "render_kw": {"rows": 4, "style": "font-family:monospace;"},
            "widget": TextArea(),
        },
        "health_check_type": {
            "label": "Health Check Type",
            "description": "none, http, or log",
        },
        "health_check_url": {
            "label": "Health Check URL",
            "description": "URL for HTTP health check (if type is http)",
        },
        "health_check_regex": {
            "label": "Health Check Regex",
            "description": "Regex for log or HTTP health check",
        },
    }
    name = "Service"
    name_plural = "Services"
    icon = "fa-solid fa-server"


class AdminAuthBackend(AuthenticationBackend):
    """Authentication backend for SQLAdmin using username and password."""

    async def login(self, request: Request) -> bool:
        """Handle login form submission."""
        form = await request.form()
        username = form.get("username")
        password = form.get("password")

        if not username or not password:
            return False

        session = SessionLocal()
        try:
            # Check if user exists and password is correct (username is case-insensitive)
            user = (
                session.query(UserModel)
                .filter(UserModel.username.ilike(username))
                .first()
            )
            if user and verify_password(password, user.password_hash):
                # Check if user has admin access
                if user.has_group("admin"):
                    # Store the actual username from database in session
                    request.session.update({"admin_username": user.username})
                    return True
            return False
        finally:
            session.close()

    async def logout(self, request: Request) -> bool:
        """Handle logout."""
        request.session.clear()
        return True

    async def authenticate(self, request: Request) -> bool:
        """Check if user is authenticated."""
        username = request.session.get("admin_username")

        if not username:
            return False

        session = SessionLocal()
        try:
            # Verify user still exists and still has admin access
            user = session.query(UserModel).filter_by(username=username).first()
            return user is not None and user.has_group("admin")
        finally:
            session.close()


def setup_admin(app, secret_key: str):
    """Setup SQLAdmin with the FastAPI app."""
    authentication_backend = AdminAuthBackend(secret_key=secret_key)
    admin = Admin(app, engine, authentication_backend=authentication_backend)
    admin.add_view(UserAdmin)
    admin.add_view(ServiceAdmin)
    return admin

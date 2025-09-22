import os
from dataclasses import dataclass, field

import sqlalchemy


@dataclass
class DatabaseConfig:
    host: str
    port: int
    database: str
    user: str
    _password: str = field(
        default=os.getenv("DB_PASSWORD", ""),
        repr=False,
        metadata={"description": "Database password, default from env var DB_PASSWORD"},
    )

    def safe_url(self, show_user: bool = True, show_password: bool = False) -> str:
        """
        Return a safe database URL, with configurable masking.
        - show_user: keep or mask the user
        - show_password: keep or mask the password
        """
        user = self.user if show_user else "****"
        password = self.password if show_password else "****"

        return f"postgresql://{user}:{password}@{self.host}:{self.port}/{self.database}"

    @property
    def password(self):
        return self._password

    def get_sqlalchemy_engine(self, **kwargs) -> sqlalchemy.engine.Engine:
        """
        Build a SQLAlchemy Engine directly.
        kwargs are passed to sqlalchemy.create_engine (e.g. pool_size, echo).
        """
        return sqlalchemy.create_engine(
            f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}", **kwargs
        )

    def __str__(self):
        return f"DatabaseConfig(url : {self.safe_url(show_password=False, show_user=False)})"

    def __repr__(self):
        return self.__str__()

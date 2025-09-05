class DatabaseConnector:
    engine: sqlalchemy.engine.Engine = None
    SessionLocal = None

    def __init__(self, database_url: str):
        if not DatabaseConnector.engine:
            DatabaseConnector.engine = create_engine(database_url, pool_pre_ping=True, pool_recycle=3600)
            DatabaseConnector.SessionLocal = sessionmaker(bind=DatabaseConnector.engine, expire_on_commit=False, autoflush=False)
            Base.metadata.create_all(DatabaseConnector.engine)

    @contextmanager
    def get_session(self):
        session = self.SessionLocal()
        try:
            yield session
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()

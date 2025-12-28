import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
# from src.monitoring.logger import structured_logger (Logger commented out to avoid AttributeError)

# Retriee the Database URL from ou setting enviroment
# We use load_dotenv to ensure our credentials are fresh from the .env file
load_dotenv()

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")

# We build the DATABASE_URL manually to avoid any string formatting issues 
# between the MonitoringConfig and the SQLAlchemy engine
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Configure the SQLAlchemy Engine
# We implement a connection pool to manage resources efficienyly on the VPS.
# pool_pre_ping=True is essential for resilient long-running scraping agents.
engine = create_engine(
    DATABASE_URL,
    pool_size=10, # Keeps up to 10 persistent connections
    max_overflow=20, # Allows 20 addicional connections during peak migration
    pool_pre_ping=True, # Checks if the connection is alive before using it
    echo=False # Set to True to debug raw SQL queries in the console
)

# Session Factory Setup
# scopped_session ensures our DB sessions are thread-safe and isolated
session_factory = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine
)
SessionLocal = scoped_session(session_factory)

def get_db_session():
    """
    Context manager for database sessions.
    Ensures the session is properly removed from the scope after use,
    preventing memory leaks or idle connection buildup on the VPS.
    """
    
    db = SessionLocal()
    try:
        return db
    finally:
        SessionLocal.remove()
        
        
def init_db():
    """
    Physical Schema Initialization.
    Translate our SQLAlchemy models into actual PoestgreSQL tables.
    Should be called during the initial setup or the migration script execution.
    """
    from src.database.models import Base
    try:
        print("🚀 Initializing datable tables... [event: db_init_start]") 
        
        # This ccommand triggers the creation of all table defined in models.py
        Base.metadata.create_all(bind=engine)
        
        print("✅ Database tables initialized successfully. [event: db_init_success]")
    except Exception as e:
        # Fixed logger call to handle the exception message correctly
        print(f"❌ Failed to initialize database: {e} [event: db_init_error]")
        raise e
import pytest
from sqlalchemy import text
from src.database.connection import engine, SessionLocal
# from src.monitoring.logger import structured_logger  <-- Comentado temporariamente para evitar o erro de atributo

def test_sqlalchemy_engine_connection():
    """
    Test 1: Low-level Connectivity.
    Verifies if the engine can connect to PostgreSQL and execute a simple query.
    """
    
    try:
        # We use a context manager to ensure the connection is closed
        with engine.connect() as connection:
            # The "SELECT 1" is the standard "ping" for SQL databases
            result = connection.execute(text("SELECT 1"))
            value = result.scalar()
            
            assert value == 1
            # structured_logger.info("Database connectivity test passed (Engine Leve).")
            print("\n✅ Database connectivity test passed (Engine Level).")
            
            
    except Exception as e:
        print(f"Connection Error: {e}")  
        pytest.fail(f"Engine could not connect to the database: {e}")
        

def test_session_creation():
    """
    Test 2: High-Level Session Management.
    Verifies if the SessionLocal factory can create a working DB session.
    """
    session = SessionLocal()
    try:
        # Verify the session is active
        assert session is not None
        # Execute a simple query through the session
        result = session.execute(text("SELECT now()")).scalar()
        assert result is not None
        
        # structured_logger.info(f"Database session test passed. Server time: {result}")
        print(f"\n✅ Database session test passed. Server time: {result}")

    except Exception as e:
        pytest.fail(f"SessionLocal couldn't establish a session: {e}")
    finally:
        session.close()
        # Clean uip the scoped session
        SessionLocal.remove()
        
if __name__ == "__main__":
    # Allow running the script directly for quick debugging
    print("Running manual connection check...")
    test_sqlalchemy_engine_connection()
    print("Connectivity OK!")
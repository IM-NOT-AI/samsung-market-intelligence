import sys
import os

# Ensuring Python finds the "src" folder when running from the root
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))) 

from src.database.connection import init_db
# from src.monitoring.logger import structured_logger

def run_setup():
    """
    Setup execution script.
    It triggers the physical creation of table in PostgreSQL based on our 
    Star Schema models.
    """
    try:
        print("🚀 Starting Database Schema Initialization...")
        
        # Callin the function declared in connection.py
        init_db()
        
        print("✅ Database setup completed! Check your PostgreSQL tables.")
        # structured_logger.info("Database migration script executed manually.", {"event": "manual_init_db"})
        
    except Exception as e:
        print(f"❌ Critical Error during setup: {e}")
        # structured_logger.info(f"Manual database init failed: {e}")
        
if __name__ == "__main__":
    run_setup()
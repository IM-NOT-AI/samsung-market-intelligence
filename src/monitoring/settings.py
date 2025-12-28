import os 
from pathlib import Path
from typing import Dict, Any
from datetime import datetime
from dotenv import load_dotenv

# Load .env (for local development)
# On the VPS, if the file is missing, it will use system enviroment variables or defaults.
load_dotenv()


class MonitoringConfig:
    """
    Centralized configuration for the Samsung Scraper monitoring and database.
    Defines logging, Prometheus metrics, and SQL connection settings.
    """
    
    # ======== Logging Configuration ========
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    
    
    # Save to the logs folder at the project root (adjusted for Docker)
    # [CHANGING HERE] name of the dynamics file with date
    current_date = datetime.now().strftime("%Y%m%d")
    LOG_FILE_PATH: Path = Path(f"logs/scraper_execution_{current_date}.log")
    
    
    LOG_ROTATION: str = "50 MB" # Rotate file when it reaches 50MB
    LOG_RETENTION: str = "30 days" # Keeping logs dor 30 days}
    
    # ======== Metrics Configuration (Prometheus) ========
    METRICS_PORT: int = int(os.getenv("METRICS_PORT", "9090"))
    
    # Performace Alerts: Warning threshhold for slow requests
    # If a page takes longer than 10s to download, generate a warning
    SLOW_REQUEST_THRESHOLD: float = 10.0
    
    @classmethod
    def get_log_config(cls) -> Dict[str, Any]:
        return {
            "level": cls.LOG_LEVEL,
            "file_path": cls.LOG_FILE_PATH,
            "rotation": cls.LOG_ROTATION,
            "retention": cls.LOG_RETENTION
        }
    
    # ======== DATABASE CONFIGURATION ========
    # Sensitive data is fetched from .env to mantain security
    DB_USER: str = os.getenv("DB_USER", "")
    DB_PASSWORD: str = os.getenv("DB_PASSWORD", "")
    DB_NAME: str = os.getenv("DB_NAME", "")
    DB_HOST: str = os.getenv("DB_HOST", "")
    DB_PORT: str = os.getenv("DB_PORT", "")

    @classmethod
    def get_db_url(cls) -> str:
        """
        Generates the SQLAlchemy connection string only if credentials are provided.
        Returns an empty string in the system is not yet configured for SQL.
        """
        if not cls.DB_USER or not cls.DB_PASSWORD:
            return ""
        return f"postgresql://{cls.DB_USER}:{cls.DB_PASSWORD}@{cls.DB_HOST}:{cls.DB_PORT}/{cls.DB_NAME}"
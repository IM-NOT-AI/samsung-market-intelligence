import os 
from pathlib import Path
from typing import Dict, Any
from datetime import datetime

class MonitoringConfig:
    """
    Centralized configuration for the Samsung Scraper monitoring.
    Defines where to save lods and Prometheus settings.
    """
    
    # Logging Configuration
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    
    
    # Save to the logs folder at the project root (adjusted for Docker)
    # [CHANGING HERE] name of the dynamics file with date
    current_date = datetime.now().strftime("%Y%m%d")
    LOG_FILE_PATH: Path = Path(f"logs/scraper_execution_{current_date}.log")
    
    
    LOG_ROTATION: str = "50 MB" # Rotate file when it reaches 50MB
    LOG_RETENTION: str = "30 days" # Keeping logs dor 30 days}
    
    # Metrics Configuration (Prometheus)
    METRICS_PORT: int = int(os.getenv("METRICS_PORT", "9090"))
    
    # Performace Alerts
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
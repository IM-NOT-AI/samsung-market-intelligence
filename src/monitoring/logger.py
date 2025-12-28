import sys
import json
from loguru import logger
from  typing import Dict, Any
from .settings import MonitoringConfig

class StructuredLogger:
    """
    Enterprise Logger for the Scrapper.
    Generates logs in JSON format for easy ingestion into  DataDog/ElasticSearch/CloudWatch.
    """
    
    def __init__(self):
        self.config = MonitoringConfig()
        self._setup_logger()
        
    def _setup_logger(self):
        """Configures Loguru to remove the default handler and use JSON"""
        logger.remove()
        
        # 1. Console Handler (To see it in the Docker terminal)
        logger.add(
            sink=sys.stdout,
            format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{function}</cyan> - <level>{message}</level>",
            level=self.config.LOG_LEVEL,
            colorize=True  
        )
        
        # 2. File Handler (Structured JSON for the future)
        if not self.config.LOG_FILE_PATH.parent.exists():
            self.config.LOG_FILE_PATH.parent.mkdir(parents=True, exist_ok=True)
        
        logger.add(
            sink=str(self.config.LOG_FILE_PATH),    
            level=self.config.LOG_LEVEL,
            rotation=self.config.LOG_ROTATION,
            retention=self.config.LOG_RETENTION,
            serialize=True, # This Generate the JSON
            enqueue=True
        )
        
    def log_business_event(self, event_name: str, **context):
        """
        Logs import business events.
        Ex: "cycle_started", "item_scraped", "cycle_completed"
        """
        logger.bind(
            event_type="business_event",
            event_name=event_name,
            context=context
        ).info(f"Event: {event_name}")
        
    
    def log_http_request(self, method: str, url: str, status_code: int, duration: float):
        """
        Logs every request the scraper makes to Mercado Livre.
        """
        level = "INFO"
        if status_code >= 400:
            level = "WARNING"
        if status_code >= 500:
            level = "ERROR"
            
        logger.bind(
            event_type="http_client_request",
            method=method,
            url=url,
            status_code=status_code,
            duration_seconds=round(duration, 4)
        ).log(level, f"{method} {url} - {status_code} - {duration:.2f}s")
        
    
    def log_error(self, error: Exception, context: Dict[str, Any] = None):
        """Logs errors with structured stacktrace."""
        logger.bind(
            event_type="error",
            error_class=type(error).__name__,
            error_message=str(error),
            context=context or {}
        ).error(f"Error: {str(error)}")
        
# Singleton intance 
structured_logger = StructuredLogger()
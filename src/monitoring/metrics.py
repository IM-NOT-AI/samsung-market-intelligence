import time
import psutil
from typing import Optional
from prometheus_client import Counter, Histogram, Gauge, CollectorRegistry, generate_latest, start_http_server
from .settings import MonitoringConfig
from .logger import structured_logger

class ScraperMetricsCollector:
    """
    Metrics collector specific for Web Scraping.
    Exposes data to Prometheus on the defined port.
    """
    
    def __init__(self):
        self.registry = CollectorRegistry()
        self._setup_metrics()
        
    def start_server(self):
        """Starts the metrics server in a separate thread"""
        try:
            start_http_server(MonitoringConfig.METRICS_PORT)
            structured_logger.log_business_event("metrics_server_started", port=MonitoringConfig.METRICS_PORT)
        except Exception as e:
            structured_logger.log_error(e, {"context": "starting_metrics_server"})
            
    def _setup_metrics(self):
        """Defines the metrics we are going to track"""
        
        # 1. Business Counters
        self.items_scraped_total = Counter(
            "samsung_items_scraped_total",
            "Total of smartphone items collected and saved",
            ["category"], # Eg.: "smartphone"
        )
        
        self.cycles_completed_total = Counter(
            "scraper_cycles_completed_total",
            "Total of full scraping cycles completed"
        )
        
        # 2. Network Performance
        self.http_requests_total = Counter(
            "scraper_http_requests_total",
            "Total HTTP requests made to target site",
            ["method", "status_code"]
        )
        
        self.http_request_duration = Histogram(
            "scraper_http_duration_seconds",
            "Time spent waiting for target response",
            buckets=(0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0)
        )
        
        # 3. Robot Health (Crucial)
        self.captcha_detected_total = Counter(
            "scraper_captcha_detected_total",
            "Number of times a captcha/softban was detected"
        )
        
        self.errors_total = Counter(
            "scraper_errors_total",
            "Total exceptions raised",
            ["type"] # Eg.: "NetworkError", "ParseError"
        ) 
        
        # 4. System Metrics (VPS CPU/RAM)
        self.system_cpu_usage = Gauge("system_cpu_usage_percent", "CPU usage percent")
        self.system_memory_usage = Gauge("system_memory_usage_bytes", "Memory usage in bytes")
        
        
    # =========== Methods to be called in scraper.py =========== #
    
    def record_http_request(self, method: str, endpoint: str, status_code: int, duration: float):
        """
        Records an HTTP request.
        NOTE: 'endpoint' is kept for compatibility but not used in labels to save cardinality.
        """
        # Fixed: Variable name matches definition (http_requests_total)
        self.http_requests_total.labels(method=method, status_code=status_code).inc()
        # Fixed: Typos corrected (duration)
        self.http_request_duration.observe(duration)
        
    def record_item_scraped(self, count: int = 1):
        """Records N items collected"""
        # Fixed: Variable name matches definition (items_scraped_total)
        self.items_scraped_total.labels(category="smartphone").inc(count)
        
    def record_captcha(self):
        """Records a block event"""
        self.captcha_detected_total.inc()
        
    def record_error(self, error_type: str):
        """"Records an error"""
        self.errors_total.labels(type=error_type).inc()
        
    def update_system_metrics(self):
        """Called periodically to update CPU/RAM"""
        self.system_cpu_usage.set(psutil.cpu_percent())
        # Fixed: .used is a property, not a function call
        self.system_memory_usage.set(psutil.virtual_memory().used)
        

# Global singleton instance
metrics = ScraperMetricsCollector()

class BusinessEventTracker:
    """
    Helper class to simplify usage in the main code.
    Unified Logging and Metrics in a single call.
    """
    
    @staticmethod
    def track_request(method: str, url: str, status: int, duration: float):
        metrics.record_http_request(method=method, endpoint="unknown", status_code=status, duration=duration)
        structured_logger.log_http_request(method, url, status, duration)
        
    @staticmethod
    def track_items(count: int):
        metrics.record_item_scraped(count)
        # Log to JSON only as an event summary, no need to log 1 by 1 to save disk space
        structured_logger.log_business_event("batch_saved", items_count=count)
        
    @staticmethod
    def track_error(e: Exception, context: str):
        error_type = type(e).__name__
        metrics.record_error(error_type)
        structured_logger.log_error(e, {"context": context})
        
    @staticmethod
    def track_scraping_start():
        structured_logger.log_business_event("scraping_cycle_started")
        
    @staticmethod
    def track_scraping_progress(page_number: int, items_found: int, total_pages: int):
        structured_logger.log_business_event(
            "page_processed",
            page=page_number,
            items=items_found,
            estimated_total_pages=total_pages
        )
    
    @staticmethod
    def track_scraping_complete(total_items: int, duration_seconds: float):
        metrics.cycles_completed_total.inc()
        structured_logger.log_business_event(
            "cycle_finished",
            total_items=total_items,
            duration_sec=round(duration_seconds, 2)
        )
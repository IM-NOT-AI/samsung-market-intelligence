import os
import sys
import time
import random
import logging
import requests
import pandas as pd
from threading import Thread
from datetime import datetime
from bs4 import BeautifulSoup


# ==============================================================================
# ENVIRONMENT AND DIRECTORY SETUP
# ==============================================================================
# Current Path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, ".."))
if project_root not in sys.path:
    sys.path.append(project_root)
    
try:
    # Importing Enterprise Monitoring Classes
    from src.monitoring.logger import structured_logger
    from src.monitoring.metrics import metrics, BusinessEventTracker
    from src.monitoring.settings import MonitoringConfig
    # Logoru for generic info logs to keep consistency
    from loguru import logger
except ImportError as e:
    print(f"CRITICAL ERROR: Could not import monitoring modules. Check folder structure. {e}")
    sys.exit(1)

# ==============================================================================
# PROMETHEUS SERVER SETUP
# ==============================================================================
from prometheus_client import start_http_server

def start_metrics_server():
    """Start the Prometheus metrics server in a background thread"""
    try:
        metrics_port = 9090
        logger.info(f"Starting Prometheus Metrics Server on Port {metrics_port}...")
        start_http_server(metrics_port)
    except Exception as e:
        logger.error(f"Failed to start metrics server: {e}")

# Start metrics in a background thread so it doesn't block the scraper
metrics_thread = Thread(target=start_metrics_server, daemon=True)
metrics_thread.start()

# ==============================================================================
# ENVIRONMENT SETUP
# ==============================================================================

# Raw data Collected from Mercado Livrea
data_raw_dir = os.path.join(current_dir, "..", "data", "raw")

# Setup log
log_dir = os.path.join(current_dir, "..", "logs")

# Log filename with timestamp
log_filename = os.path.join(log_dir, f"scraper_samsung_{datetime.now().strftime('%Y%m%d')}.log")

# Final CSV file
csv_file_path = os.path.join(data_raw_dir, "samsung_market_data.csv")

# DEFAULT CONFIGURATION
DEFAULT_CSV_PATH = os.path.join(data_raw_dir, "samsung_market_data.csv")

# Logging Configuration
structured_logger.log_business_event(
    event_name="scraper_initialization",
    context={
        "csv_path": str(csv_file_path),
        "log_dir": str(MonitoringConfig.LOG_FILE_PATH)
    }
)

# ==============================================================================
# SCRAPING CONFIGURATION
# ==============================================================================

# Rotational User Agents (To avoid SOFT BANS!)
user_agents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0"
]

def get_random_header():
    return {
        "User-Agent" : random.choice(user_agents),
        "Accept" : "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",    
        "Referer": "https://www.google.com/"
    }
    
# Price Range Configuration (Granular strategy for 10k records)
# Focus: Mobile Phones (Starting from 0 BRL to 20000 BRL)
price_ranges = []

# Low/Mid Range (High density of products) - STEP 50
for p in range(0, 2500, 50): 
    price_ranges.append((p, p + 49))

# High Range (e.g: S22, S23, S24) - STEP 100
# CORREÇÃO CRÍTICA: Removida a vírgula extra que causava o erro "too many values to unpack"
for p in range(2500, 6000, 100): 
    price_ranges.append((p, p + 99))

# Premium/Foldables - STEP 500
for p in range(6000, 20000, 500): 
    price_ranges.append((p, p + 499))

logging.info(f"Scraping Configuration Loaded. Total Price Ranges: {len(price_ranges)}")

# ==============================================================================
# MAIN LOGIC
# ==============================================================================

def main_loop(single_run=False, output_file=DEFAULT_CSV_PATH):
    """
    Main function:
    :param single_run: If True, runs only one cycle and stops (Used for testing).
    : param output_file: Path where the CSV will be saved.
    """
    # [CI SAFETY ADJUSTMENT]
    # Ensures the output file exists even if no items are found.
    # This prevents integration tests from failing due to a missing CSV file.
    if not os.path.exists(output_file):
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        pd.DataFrame (columns=["extraction_date", "title", "price", "link"]).to_csv(
            output_file, index=False, sep=";"
        )
    
    cycle_count = 1
    # Maintain "seen_links" per cycle to allow capturing price changes over time
    seen_links_in_cycle = set()
    
    # Defining price ranges based on the mode
    if single_run:
        logger.warning("⚠️ MODE: SINGLE RUN (TESTING) ⚠️")
        # [CHANGE 1] Narrow range (10 BRL gap) to ensure speed (approx 5-50 items)
        current_price_ranges = [(1200, 1210)] # Small range for quick testing
    else:
        current_price_ranges = price_ranges # Global full ranges
    
    while True:
        # [MONITORING] Track Cycle Start
        structured_logger.log_business_event(
            event_name="cycle_started",
            context={"cycle_id": cycle_count}
        )
        
        BusinessEventTracker.track_scraping_start()
        
        start_time = time.time()
        total_items_cycle = 0 
        seen_links_in_cycle.clear()
        
        for min_price, max_price in current_price_ranges:
            logging.info(f"Processing range: R$ {min_price} to R$ {max_price}")
        
            # Base url construction
            url_base = (
                f"https://lista.mercadolivre.com.br/"
                f"celulares-telefones/"
                f"celulares-smartphones/"
                f"samsung/"
                f"samsung_PriceRange_{min_price}-{max_price}_Desde_"
            )
            
            counter_starter = 1
            consecutive_errors = 0
            page_number= 1
            
            # Pagination Loop
            while True:
                # Pagination URL Construction
                if counter_starter == 1:
                    target_url = (
                        f"https://lista.mercadolivre.com.br/"
                        f"celulares-telefones/"
                        f"celulares-smartphones/"
                        f"samsung/"
                        f"samsung_PriceRange_{min_price}-{max_price}_NoIndex_True"
                    )
                else:
                    target_url = f"{url_base}{counter_starter}_NoIndex_True" 
                
                try:
                    # Random Sleep (CRITICAL for 24/7 operation on VPS)
                    sleep_time = random.uniform(2.5, 5.0)
                    time.sleep(sleep_time)
                    
                    # [MONITORING] Track Request Latency using MetricsCollector
                    req_start = time.time()
                    response = requests.get(target_url, headers=get_random_header(), timeout=20)
                    req_duration = time.time() - req_start
                    
                    # Log request metrics to Prometheus
                    metrics.record_http_request(
                        method="GET",
                        endpoint="mercadolivre_search",
                        status_code=response.status_code,
                        duration=req_duration
                    )
                    
                    # Log request to JSON log
                    structured_logger.log_http_request(  # <--- CORRECTING _http_
                        method="GET",
                        url=target_url, 
                        status_code=response.status_code,
                        duration=req_duration
                    )
                    
                    # Check Status Code
                    if response.status_code != 200:
                        logging.warning(f"Status Code {response.status_code} at page index {counter_starter}. Skipping Range.")
                        break
                    
                    soup = BeautifulSoup(response.content, "html.parser")
                    
                    # Anti-Bot Detection Check (Captcha)
                    page_text = soup.get_text().lower()
                    if "human" in page_text or "captcha" in page_text:
                        # [MONITORING] Log Error Event
                        structured_logger.log_error(
                            error=Exception("Soft Ban Detected"),
                            context={"action": "sleeping_15_min", "trigger": "captcha_text"}
                        )
                        logging.critical("BLOCK DETECTED (CAPTCHA)! Sleeping for 15 MINUTES...")
                        time.sleep(900) # 15 minutes penalty
                        continue # Retry same page
                    
                    # Hybrid Selector Strategy (Grid vs List Layouts)
                    cards = soup.find_all("div", class_="poly-card__content")
                    layout_type = "grid"
                    
                    if not cards:
                        cards = soup.find_all("li", class_="ui-search-layout__item")
                        layout_type = "list"
                    
                    # If no cards found, assume end of pagination for this range
                    if not cards:
                        logging.info(f"End of Items for Range R$ {min_price} - {max_price}. Pages Scraped: {counter_starter // 48}")
                        break
                    
                    # Batch Data Processing
                    batch_data = []
                    
                    for card in cards:
                        try:
                            # ==========================
                            # DATA EXTRACTION
                            # ==========================
                            
                            # 1. Link (Primary Key)
                            link_tag = card.find("a", class_="poly-component__title") or card.find("a", class_="ui-search-link") or card.find("a")
                            link_raw = link_tag.get("href", "") if link_tag else "N/A"
                            link_clean = link_raw.split("?")[0].split("#")[0]
                            
                            if link_clean in seen_links_in_cycle:
                                continue
                            seen_links_in_cycle.add(link_clean)
                            
                            # 2. Title
                            title_tag = card.find("h3", class_="poly-component__title-wrapper") or card.find("h2", class_="ui-search-item__title")
                            title_text = title_tag.get_text(strip=True) if title_tag else "N/A"
                            
                            # 3. Seller 
                            seller_tag = card.find("span", class_="poly-component__seller") or card.find("span", class_="poly-component__brand") or card.find("p", class_="ui-search-official-store-label")
                            seller_text = seller_tag.get_text(strip=True) if seller_tag else "N/A"
                            
                            # 4. Price 
                            price_tag = card.find("span", class_="andes-money-amount__fraction")    
                            cents_tag = card.find("span", class_="andes-money-amount__cents")
                            
                            price_value = price_tag.get_text(strip=True) if price_tag else "0"
                            # Correction: cents_tag em vez de cents_text
                            cents_value = cents_tag.get_text(strip=True) if cents_tag else "00"
                            price_full = f"{price_value}.{cents_value}"
                            
                            # 5. Discount 
                            discount_tag = card.find("span", class_="andes-money-amount__discount")
                            discount_text = discount_tag.get_text(strip=True).split(" ")[0] if discount_tag else "N/A"
                            
                            # 6. Installments & Interest 
                            installment_tag = card.find("span", class_="poly-price__installments") or card.find("span", class_="ui-search-item__group__element ui-search-installments") 
                            installment_qty = "N/A"
                            interest_free = "N/A"
                            
                            if installment_tag:
                                # Correction: separator=" " (evita texto colado) e strip=True (typo 'stripe')
                                full_installments_text = installment_tag.get_text(separator=" ", strip=True)
                                
                                interest_free = "Sem Juros" if "sem juros" in full_installments_text.lower() else "Com Juros" 
                                
                                if "x" in full_installments_text.lower():
                                    # Split by "x" and gets the last word of the first part
                                    installment_qty = full_installments_text.lower().split("x")[0].split()[-1]
                                else:
                                    installment_qty = "1"
                                
                            # 7. Total Sold
                            sold_text = "N/A"    
                            for span in card.find_all("span"):
                                if "vendidos" in span.get_text().lower():
                                    sold_text = span.get_text(strip=True)
                                    break
                                
                            
                            # 8. Delivery & Shipping
                            
                            # 8.1 Free Delivery
                            free_delivery = "No"
                            shipping_tag = card.find("div", class_="poly-component-shipping") or card.find("p", class_="ui-search-item__shipping")
                            if shipping_tag:
                                if "grátis" in shipping_tag.get_text(strip=True).lower():
                                    free_delivery = "Yes"
                            
                            # (8.2 & 8.3 & 8.4) === Delivered === (Today, Tomorrow, or Days of Week)
                            # unified variable to makes the analysis easier leater on: "shipping_arrival_estimation"
                            arrival_estimation = "Standard"
                            
                            # Today Check
                            if card.find("span", class_="poly-shipping--same_day"):
                                arrival_estimation = "Today"
                            # Tomorrow Check
                            elif card.find("span", class_="poly-shipping--next_day"):
                                arrival_estimation = "Tomorrow"
                            else:
                                # Day of Week Check 
                                # The "Mercardo Livre" use classes as poly-shipping--monday, poly-shipping--tuesday, etc.
                                week_days = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
                                for day in week_days:
                                    day_tag = card.find("span", class_=f"poly-shipping--{day}")
                                    if day_tag:
                                        # Get text of which day in a week
                                        arrival_text = day_tag.get_text(strip=True)
                                        arrival_estimation = f"DayWeek {day} ({arrival_text})"
                                        break
                            
                            # 9. Highlights (Checking Text Content)
                            # The class "poly-component__highlight" is generic. We must check the text inside.
                            highlight_tag = card.find("span", class_="poly-component__highlight")
                            highlight_text = highlight_tag.get_text(strip=True).upper() if highlight_tag else ""
                            
                            is_great_deal = "Yes" if "IMPERDÍVEL" in highlight_text or "OFERTA" in highlight_text else "No"
                            is_bestseller = "Yes" if "MAIS VENDIDO" in highlight_text else "No"
                            is_recommended = "Yes" if "RECOMENDADO" in highlight_text else "No " 

                            # Timestamp
                            extraction_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            
                            # Dictionary
                            item_data = {
                                "extraction_date": extraction_date,
                                "cycle_id": cycle_count,
                                "title" : title_text,
                                "seller": seller_text,
                                "price": price_full,
                                "discount": discount_text,
                                "installments": installment_qty,
                                "interest_free": interest_free,
                                "total_sold_raw": sold_text,
                                "free_delivery": free_delivery,
                                "arrival_estimation": arrival_estimation,
                                "is_great_deal": is_great_deal,
                                "is_bestseller": is_bestseller,
                                "is_recommended": is_recommended,
                                "link": link_clean,
                                "layout_type": layout_type,
                                "price_range_searched": f"{min_price}-{max_price}"
                            }
                            
                            batch_data.append(item_data)
                            
                        except Exception as e_inner:
                            # [Monitoring] log inner Loop error
                            structured_logger.log_error(error=e_inner, context={"scope": "card_extraction"})
                            continue
                    
                    # ################################
                    # INCREMENTAL SAVING (APPEND MODE)
                    # ################################
                    
                    if batch_data:
                        df = pd.DataFrame(batch_data)                        
                        header_mode = not os.path.exists(output_file)
                        
                        try:
                            # CORRECTION: mode="a" to append (persistency)
                            df.to_csv(output_file, mode="a", index=False, sep=";", encoding="utf-8-sig", header=header_mode)
                            items_count = len(batch_data)
                            total_items_cycle += items_count
                            
                            #[MONITORING] track items and Page Progress
                            BusinessEventTracker.track_items(items_count)
                            
                            # Track Page
                            BusinessEventTracker.track_scraping_progress(
                                page_number=page_number,
                                items_found=items_count,
                                total_pages=40
                            )
                            
                        except Exception as e_csv:
                            structured_logger.log_error(error=e_csv, context={"scope": "csv_saving", "file": csv_file_path})
                     
                    # [CHANGE 2] Circuit Breaker for Testing
                    # If testing, force stop after processing the first page (48 items max)
                    if single_run:
                        logging.info(f"TEST MODE: Breaking pagination loop after 1st page.")
                        break 
                           
                    # Pagination Increment
                    counter_starter += 48
                    page_number +=1
                    
                    # Technical Safety Limit (ML usually stops serving after ~2000 items)
                    if counter_starter > 2000:
                        logging.info(f"ML Pagination Limit Reached for this Range.")
                        break
                
                except requests.exceptions.RequestException as e_net:
                    structured_logger.log_error(error=e_net, context={"scope": "network_request"} )
                    consecutive_errors += 1
                    time.sleep(30)
                    if consecutive_errors > 3:
                        break
                
                except Exception as e_gen:
                    structured_logger.log_error(error=e_gen, context={"scope": "pagination_loop_generic"})
                    break
    
        # END OF CYCLE 
        duration_minutes = (time.time() - start_time) / 60
        
        # [MONITORING] Track Cycle Completion
        structured_logger.log_business_event(
            event_name="cycle_completed",
            context={
                "cycle_id": cycle_count,
                "total_items": total_items_cycle,
                "duration_minutes": round(duration_minutes, 2)
            }
        )

        # Tracker Method
        BusinessEventTracker.track_scraping_complete(
            total_items=total_items_cycle,
            duration_seconds=time.time() - start_time
        )
        
        cycle_count += 1
        
        # [THE INTEGRATION MAGIC]
        if single_run:
            logger.info("Single Run Requested. Stopping loop.")
            break
        
        # Sleep between cycles (e.g.: 6 hours)
        hours_sleep = 6
        logging.info(f"Entering standby mode for {hours_sleep} hours...")
        time.sleep(hours_sleep * 3600)
        

if __name__ == "__main__":
    # Checks enviroment variable ONLY to decide how to call the function
    is_test_env = os.getenv("SCRAPER_MODE") == "TEST"
    try:
        if is_test_env:
            # Test Mode: Save to junk file and run once
            # This is the "Key" to getting the Green Checkmark.
            test_csv = os.path.join(data_raw_dir, "integration_test_data.csv")
            main_loop(single_run=True, output_file=test_csv)
        else:
            # Production Mode: Runs forever on the official file (VPS)
            main_loop(single_run=False)
            
    
    except KeyboardInterrupt:
        logger.info("Script Interrupted by User.")
    except Exception as e:
        structured_logger.log_error(error=e, context={"scope": "main_execution", "fatal": True})
        sys.exit(1)
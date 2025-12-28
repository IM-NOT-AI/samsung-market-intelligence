import pandas as pd
import sys
import os
from datetime import datetime

# Path setup to ensure "src" is discoverable
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.database.connection import SessionLocal
from src.database.models import DimProduct, DimSeller, DimScraperMetadata, FactOffer
# from src.monitoring.logger import structured_logger (Disabled to avoid AttributeError)

def sanitize_price(value):
    """
    Helper function to fix currency formatting issues.
    Handles:
    - '1.099.99' -> 1099.99 (Removes first dot)
    - '1.200,50' -> 1200.50 (Brazilian format)
    - 'R$ 1000'  -> 1000.0 (Currency symbols)
    """
    if pd.isna(value) or str(value).strip().lower() in ['n/a', 'nan', '']:
        return 0.0
    
    # Convert to string and basic cleaning
    s = str(value).replace("R$", "").replace("$", "").strip()
    
    # Case 1: Brazilian format (1.000,00) -> Convert to 1000.00
    if "," in s:
        s = s.replace(".", "").replace(",", ".")
    
    # Case 2: Dirty format with multiple dots (1.099.99) -> Remove all dots except the last one
    elif s.count(".") > 1:
        # Replaces all dots with empty string, up to the last one
        s = s.replace(".", "", s.count(".") - 1)
        
    try:
        return float(s)
    except ValueError:
        print(f"⚠️ Warning: Could not parse price '{value}', defaulting to 0.0")
        return 0.0

def migrate_data():
    """
    Core Migration Logic: CSV -> PostgreSQL.
    Follows BR-03 (Idempotency) and BR-04 (Sanitization).
    """
    csv_path = "data/raw/samsung_market_data.csv"
    
    if not os.path.exists(csv_path):
        print(f"❌ Migration failed: {csv_path} not found")
        return
    
    # Start DB Session
    session = SessionLocal()
    
    try:
        # Load and Sanitize Data using Pandas
        df = pd.read_csv(csv_path, sep=";", encoding='utf-8')
        print(f"📊 Starting migration of {len(df)} rows...")
        
        counter = 0
        for _, row in df.iterrows():
            # ======= Dimension: SCRAPER METADATA =======
            # Check if cycle exists or create it
            metadata = session.query(DimScraperMetadata).filter_by(cycle_id=row["cycle_id"]).first()
            if not metadata:
                metadata = DimScraperMetadata(
                    cycle_id=row["cycle_id"],
                    layout_type=row["layout_type"],
                    price_range_searched=row["price_range_searched"]
                )
                session.add(metadata)
                session.flush() # Sync to get the ID for foreign keys
                
                
            # ======= Dimension: SELLER =======
            # Normalizing Seller name as per Business Rule BR-06
            seller_name = "Unknown Seller" if pd.isna(row["seller"]) or str(row["seller"]) == "N/A" else str(row["seller"])
            seller = session.query(DimSeller).filter_by(seller_name=seller_name).first()
            if not seller:
                seller = DimSeller(seller_name=seller_name)
                session.add(seller)
                session.flush()
            
            # ======= Dimension: PRODUCT =======
            # Using the Link as the Natural Key (BR-07)
            product = session.query(DimProduct).filter_by(sku_link=row["link"]).first()
            if not product:
                product = DimProduct(
                    sku_link=row["link"],
                    title=str(row["title"])
                )
                session.add(product)
                session.flush()
                
            # ======= Dimension: OFFER =======
            # Idempotency check: Don't duplicate products in the same cycle (BR-03)
            existing_offer = session.query(FactOffer).filter_by(
                product_id=product.product_id,
                cycle_id=metadata.cycle_id,
                seller_id=seller.seller_id
            ).first()
            
            if not existing_offer:
                # Sanitizing price to handle format '1.099.99' or '1.000,00'
                price_value = sanitize_price(row["price"])
                
                new_offer = FactOffer(
                    product_id=product.product_id,
                    seller_id=seller.seller_id,
                    cycle_id=metadata.cycle_id,
                    price=price_value, 
                    discount=str(row["discount"]),
                    # Safe conversion for installments (handling '10.0' strings)
                    installments=0 if pd.isna(row["installments"]) or str(row["installments"]) == "N/A" else int(float(row["installments"])),
                    total_sold_raw=str(row["total_sold_raw"]),
                    arrival_estimation=str(row["arrival_estimation"]),
                    interest_free=(str(row["interest_free"]).strip().lower() == "sem juros"),
                    free_delivery=(str(row["free_delivery"]) == "Yes"),
                    is_great_deal=(str(row["is_great_deal"]) == "Yes"),
                    is_bestseller=(str(row["is_bestseller"]) == "Yes"),
                    is_recommended=(str(row["is_recommended"]) == "Yes"),
                    extraction_date=datetime.strptime(row["extraction_date"], "%Y-%m-%d %H:%M:%S")
                )
                session.add(new_offer)
                counter += 1
                
        # Commit Transaction
        session.commit()
        print(f"✅ Data migration finished! {counter} new offers inserted.")
            
    except Exception as e:
        # Atomic transaction: if one fails, we rollback the whole batch
        session.rollback()
        print(f"❌ Critical error during migration: {e}")
    finally:
        session.close()
        
if __name__ == "__main__":
    migrate_data()
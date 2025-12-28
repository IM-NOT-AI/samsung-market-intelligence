from sqlalchemy import Column, String, Float, DateTime, Integer, Boolean, ForeignKey, Index
from sqlalchemy.orm import relationship, declarative_base
from datetime import datetime

# Base class for SQLAlchemy models
# Inheriting from declarative_base links the classes to the DB schema
Base = declarative_base()


class DimProduct(Base): # Fixed: Singular name to match logic
    """
    Dimension Table: DIM_PRODUCTS
    Normalizes product data to avoid string redudancy in the Fact Table.
    The sku_link is the natural key for cross_cycle tracking.
    """
    
    __tablename__ = "dim_products"
    
    product_id = Column(Integer, primary_key=True, autoincrement=True)
    # Sanitized unique URL - serves as our Natural Key (UK)
    sku_link = Column(String(2048), unique=True, nullable=False, index=True)
    # The latest title captured for this product
    title = Column(String(500), nullable=False)
    
    # Relationship back to the Fact table for easy joining via ORM 
    offers = relationship("FactOffer", back_populates="product")
    
    def __repr__(self):
        return f"<DimProduct(title='{self.title[:30]}...', id={self.product_id})>"
    
class DimSeller(Base):
    """
    Dimension Table: DIM_SELLERS
    Maintains a single source of truth for market participants (Sellers).
    """
    
    __tablename__ = "dim_sellers"
    
    seller_id = Column(Integer, primary_key=True, autoincrement=True)
    # Unique store name (e.g., "Samsung", "Fast Shop")
    seller_name = Column(String(255), unique=True, nullable=False, index=True)
    
    offers = relationship("FactOffer", back_populates="seller")
    
class DimScraperMetadata(Base):
    """
    Dimension Table: DIM_SCRAPER_METADATA
    Techinical dimension used for operational audit data lineage.
    """
    
    __tablename__ = "dim_scraper_metadata"
    
    cycle_id = Column(Integer, primary_key=True) # ID provided by the scraper cycle
    layout_type = Column(String(50)) # "grid" vs "list"
    price_range_searched = Column(String(100)) # Bracket (e.g., "1000-1050")
    cycle_start = Column(DateTime, default=datetime.now)
    
    offers = relationship("FactOffer", back_populates="metadata_obj") # Fixed back_populates
    
    
class FactOffer(Base):
    """
    Fact Table: FACT_OFFERS
    The metrics core of the Star Schema.
    Stores snapshots of price, availability, and platform badges.
    """
    
    __tablename__ = "fact_offers"
    
    offer_id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Foreign Keys (The backbone of the Star Schema)
    product_id = Column(Integer, ForeignKey("dim_products.product_id"), nullable=False)
    seller_id = Column(Integer, ForeignKey("dim_sellers.seller_id"), nullable=False)
    cycle_id = Column(Integer, ForeignKey("dim_scraper_metadata.cycle_id"), nullable=False)
    
    # Snapshot Metrics (Capture at VPS extraction time)
    price = Column(Float, nullable=False, index=True)
    discount = Column(String(50))
    installments = Column(Integer)
    
    # Flags mapped to Boolean as per BR-05 (Storage Optimization)
    interest_free = Column(Boolean, default=False)
    free_delivery = Column(Boolean, default=False)
    is_great_deal = Column(Boolean, default=False)
    is_bestseller = Column(Boolean, default=False)
    is_recommended = Column(Boolean, default=False)
    
    # Logistics
    total_sold_raw = Column(String(100))
    arrival_estimation = Column(String(255))
    
    
    # Original timestamp from VPS
    extraction_date = Column(DateTime, nullable=False, index=True)
    
    # ORM Relationships for simplified querying
    product = relationship("DimProduct", back_populates="offers")
    seller = relationship("DimSeller", back_populates="offers")
    metadata_obj = relationship("DimScraperMetadata", back_populates="offers") # Fixed variable name
    
    # Composite Index for commom analytics (Time Series Performace)
    __table_args__ = ( # Fixed typo from __tabl_args__
        Index("idx_product_extraction", "product_id", "extraction_date"),
    )
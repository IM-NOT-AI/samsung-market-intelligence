import pytest
from bs4 import BeautifulSoup

# Simulating a piece of Maercado Livre HTML (Mock)
HTML_MOCK_OFFER = """
<div class="poly-card__content">
    <h2 class="ui-search-item__title"> Samsung Galaxy S22</h2>
    <span class="poly-component__highlight">OFERTA DO DIA</span>
</div>
"""

HTML_MOCK_REGULAR = """
<div class="poly-card__content">
    <h2 class="ui-search-item__title">Samsung Galaxy A03</h2>
    </div>
"""

def test_detects_unmissable_offer():
    """Test if logic identifies OFFER correctly"""
    soup = BeautifulSoup(HTML_MOCK_OFFER, "html.parser")
    card = soup.find("div", class_="poly-card__content")
    
    
    # Main Logic for Scraper.py -> New insertion of Data
    highlight_tag = card.find("span", class_="poly-component__highlight")
    hightlight_text = highlight_tag.get_text(strip=True).upper() if highlight_tag else ""
     
    is_great_deal = "Yes" if "IMPERDÍVEL" in hightlight_text or "OFERTA" in hightlight_text else "No"
    
    assert is_great_deal == "Yes"
    assert hightlight_text == "OFERTA DO DIA"
    
def test_product_without_highlight():
    """Tests if regular product does not get false highlight"""
    soup = BeautifulSoup(HTML_MOCK_REGULAR, "html.parser")
    card = soup.find("div", class_="poly-card__content")
    
    highlight_tag = card.find("span", class_="poly-component__highlight")
    highlight_text = highlight_tag.get_text(strip=True).upper() if highlight_tag else ""
    
    is_great_deal = "Yes" if "OFERTA" in highlight_text else "No"
    
    assert is_great_deal == "No"
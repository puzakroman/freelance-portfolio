import requests
from bs4 import BeautifulSoup
import csv
import logging
import time

# Configure logging for production-ready monitoring
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class DataExtractor:
    """
    A robust web crawler designed for efficient data extraction from 
    e-commerce platforms with built-in error handling and CSV export.
    """
    def __init__(self, base_url):
        self.base_url = base_url
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        self.results = []

    def fetch_page_content(self, url):
        """Fetches HTML content with error handling and timeout management."""
        try:
            response = requests.get(url, headers=self.headers, timeout=10)
            response.raise_for_status()
            return response.text
        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to retrieve data from {url}: {e}")
            return None

    def parse_data(self, html):
        """Processes raw HTML and extracts key item attributes."""
        soup = BeautifulSoup(html, 'html.parser')
        items = soup.find_all('article', class_='product_pod')
        
        for item in items:
            title = item.h3.a['title']
            price = item.find('p', class_='price_color').text
            stock_status = item.find('p', class_='instock availability').text.strip()
            
            self.results.append({
                'Item Title': title,
                'Price': price,
                'Availability': stock_status
            })
        
        return len(items)

    def export_to_csv(self, filename='extracted_data.csv'):
        """Exports the collected dataset to a structured CSV file."""
        if not self.results:
            logging.warning("No data extracted during the session.")
            return

        fieldnames = self.results[0].keys()
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(self.results)
        logging.info(f"Data export successful. {len(self.results)} records saved to {filename}")

if __name__ == "__main__":
    # Target URL for demonstration (using a standard scraping sandbox)
    DEMO_URL = "http://books.toscrape.com/catalogue/page-1.html"
    
    extractor = DataExtractor(DEMO_URL)
    logging.info("Initializing extraction process...")
    
    raw_html = extractor.fetch_page_content(DEMO_URL)
    if raw_html:
        extracted_count = extractor.parse_data(raw_html)
        logging.info(f"Successfully parsed {extracted_count} items from source.")
        extractor.export_to_csv('market_data_export.csv')
    
    logging.info("Process completed.")

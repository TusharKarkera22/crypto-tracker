import requests
import gspread
import pandas as pd
import schedule
import time
import logging
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def authenticate_google_sheets(sheet_name):
    try:
        gc = gspread.service_account(filename="credentials.json")
        return gc.open(sheet_name).sheet1
    except Exception as e:
        logger.error(f"Google Sheets authentication error: {e}")
        raise

# Authenticate Google APIs for Docs
def authenticate_google_docs():
    try:
        scopes = [
            "https://www.googleapis.com/auth/documents",
            "https://www.googleapis.com/auth/drive"
        ]
        credentials = Credentials.from_service_account_file("credentials.json", scopes=scopes)
        return build("docs", "v1", credentials=credentials)
    except Exception as e:
        logger.error(f"Google Docs authentication error: {e}")
        raise


def fetch_crypto_data():
    try:
        url = "https://api.coingecko.com/api/v3/coins/markets"
        params = {
            "vs_currency": "usd",
            "order": "market_cap_desc",
            "per_page": 50,
            "page": 1,
            "sparkline": False
        }
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logger.error(f"Error fetching crypto data: {e}")
        return []

# Update Google Sheet with live data
def update_google_sheet(sheet):
    try:
        data = fetch_crypto_data()
        if not data:
            logger.warning("No crypto data retrieved")
            return

        rows = [
            ["Cryptocurrency Name", "Symbol", "Price (USD)", "Market Cap", "24h Volume", "24h Change (%)"]
        ]
        for item in data:
            rows.append([ 
                item["name"],
                item["symbol"].upper(),
                f"${item['current_price']:,.2f}",
                f"${item['market_cap']:,}",
                f"${item['total_volume']:,}",
                f"{item['price_change_percentage_24h']:.2f}%"
            ])

        sheet.clear()
        sheet.update(rows)
        logger.info("Google Sheet updated successfully!")
    except Exception as e:
        logger.error(f"Error updating Google Sheet: {e}")

# Update Google Docs with analysis report
def update_google_docs_report(doc_service, doc_id):
    try:
        data = fetch_crypto_data()
        if not data:
            logger.warning("No crypto data retrieved")
            return

        df = pd.DataFrame(data)

        # Perform analysis
        top_5 = df.nlargest(5, "market_cap")[["name", "market_cap"]]
        average_price = df["current_price"].mean()
        max_change = df["price_change_percentage_24h"].max()
        min_change = df["price_change_percentage_24h"].min()

        # Prepare report content
        report_content = (
            f"Cryptocurrency Analysis Report\n"
            f"================================\n\n"
            f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
            f"Top 5 Cryptocurrencies by Market Cap:\n"
            f"-----------------------------------\n"
        )

        for _, row in top_5.iterrows():
            report_content += f"{row['name']} - Market Cap: ${row['market_cap']:,}\n"

        report_content += (
            f"\nMarket Overview:\n"
            f"---------------\n"
            f"Average Price of Top 50 Cryptocurrencies: ${average_price:.2f}\n"
            f"Highest 24h Change: {max_change:.2f}%\n"
            f"Lowest 24h Change: {min_change:.2f}%\n"
        )

      
        logger.debug(f"Report Content:\n{report_content}")

      
        document = doc_service.documents().get(documentId=doc_id).execute()
        logger.debug(f"Document title: {document.get('title')}")

      
        requests = [
            {
                'insertText': {
                    'location': {'index': 1},
                    'text': report_content
                }
            }
        ]

       
        result = doc_service.documents().batchUpdate(
            documentId=doc_id,
            body={'requests': requests}
        ).execute()

        logger.info("Google Docs analysis report updated successfully!")
        
    except Exception as e:
        logger.error(f"Error updating Google Docs report: {e}")
      
        import traceback
        traceback.print_exc()

def update_data_and_report(sheet, doc_service, doc_id):
    update_google_sheet(sheet)
    update_google_docs_report(doc_service, doc_id)


def main():
    try:
       
        sheet_name = "Crypto Live Data"
      
        google_doc_id = "1Igu_5PWqavJfkAIy6-025kK33WUrdoHQbW1GuQj968A"  

        
        sheet = authenticate_google_sheets(sheet_name)
        doc_service = authenticate_google_docs()

     
        schedule.every(5).minutes.do(lambda: update_google_sheet(sheet))  # Update Google Sheet every 5 minutes
        schedule.every(24).hours.do(lambda: update_google_docs_report(doc_service, google_doc_id))  # Update Google Docs every 24 hours

        while True:
            schedule.run_pending()
            time.sleep(1)

    except Exception as e:
        logger.error(f"Critical error in main function: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()

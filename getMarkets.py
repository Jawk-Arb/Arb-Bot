import json
import requests
import pandas as pd
from datetime import date, datetime
import logging
import time
import os
from dotenv import load_dotenv

load_dotenv()

class PolyMarketAPI:
    def __init__(self):
        self.BASE_URL = "https://gamma-api.polymarket.com"
        self.PAGE_SIZE = 100
        self.OUTPUT_FILE = 'polymarket_data.csv'
        
    def _parse_market_data(self, market):
        try:
            return {
                    'id': market.get('id', ''),
                    'question': market.get('question', ''),
                    'condition_id': market.get('conditionId', ''),
                    'slug': market.get('slug', ''),
                    'end_date': datetime.strptime(market.get('endDate', '2000-01-01T00:00:00Z'), '%Y-%m-%dT%H:%M:%SZ').date(),
                    'category': market.get('category', ''),
                    'liquidity': float(market.get('liquidity', 0)),
                    'description': market.get('description', ''),
                    'outcomes': json.loads(market.get('outcomes', '[]')),
                    'outcome_prices': json.loads(market.get('outcomePrices', '[]')),
                    'volume': float(market.get('volume', 0)),
                    'active': market.get('active', False),
                    'market_type': market.get('marketType', ''),
                    'closed': market.get('closed', False),
                    'market_maker_address': market.get('marketMakerAddress', ''),
                    'updated_by': market.get('updatedBy', 0),
                    'created_at': market.get('createdAt', ''),
                    'updated_at': market.get('updatedAt', ''),
                    'closed_time': market.get('closedTime', ''),
                    'archived': market.get('archived', False),
                    'restricted': market.get('restricted', False),
                    'volume_num': float(market.get('volumeNum', 0)),
                    'liquidity_num': float(market.get('liquidityNum', 0)),
                    'has_reviewed_dates': market.get('hasReviewedDates', False),
                    'ready_for_cron': market.get('readyForCron', False),
                    'volume_24hr': float(market.get('volume24hr', 0)),
                    'clob_token_ids': market.get('clobTokenIds', '[]'),
                    'fpmm_live': market.get('fpmmLive', False),
                    'competitive': float(market.get('competitive', 0)),
                    'spread': float(market.get('spread', 0)),
                    'one_day_price_change': float(market.get('oneDayPriceChange', 0)),
                    'last_trade_price': float(market.get('lastTradePrice', 0)),
                    'best_bid': float(market.get('bestBid', 0)),
                    'best_ask': float(market.get('bestAsk', 0)),
                    'clear_book_on_start': market.get('clearBookOnStart', False)
                }
        except Exception as e:
            logging.error(f"Error parsing market data: {e}")
            return None

    def get_markets(self):
        all_markets = []
        offset = 0
        today = date.today().isoformat()

        while True:
            try:
                params = {
                    'limit': self.PAGE_SIZE,
                    'offset': offset,
                    'end_date_min': today,
                    'active': True,
                    'closed': False
                }
                
                response = requests.get(f"{self.BASE_URL}/markets", params=params)
                response.raise_for_status()
                
                markets = response.json()
                if not markets:
                    break
                    
                parsed_markets = [
                    data for data in (self._parse_market_data(market) for market in markets)
                    if data is not None
                ]
                all_markets.extend(parsed_markets)
                offset += self.PAGE_SIZE
                
            except requests.RequestException as e:
                logging.error(f"API request failed: {e}")
                break
            except Exception as e:
                logging.error(f"Unexpected error: {e}")
                break

        return pd.DataFrame(all_markets)

    def save_to_csv(self, markets_data):
        try:
            df = pd.DataFrame(markets_data)
            df.to_csv(self.OUTPUT_FILE, index=False)
            return len(markets_data)
        except Exception as e:
            logging.error(f"Error saving to CSV: {e}")
            return 0

# Kalshi API
class KalshiAPI:
    def __init__(self, email, password):
        self.BASE_URL = "https://trading-api.kalshi.com/trade-api/v2"
        self.MARKETS_URL = "https://api.elections.kalshi.com/trade-api/v2/markets"
        self.email = email
        self.password = password
        self.token = self._get_token()
    
    def _get_token(self):
        payload = {
            "email": self.email,
            "password": self.password
        }
        headers = {
            "accept": "application/json",
            "content-type": "application/json"
        }
        response = requests.post(f"{self.BASE_URL}/login", json=payload, headers=headers)
        return response.json()["token"]
    
    def get_markets(self):
        unix = int(time.time())
        cursor = None
        markets = pd.DataFrame()
        
        headers = {
            "accept": "application/json",
            "Authorization": f"Bearer {self.token}"
        }
        
        while True:
            params = {
                "limit": 1000,
                "min_close_ts": unix,
                "cursor": cursor,
                "status": "open"
            }
            
            try:
                response = requests.get(self.MARKETS_URL, headers=headers, params=params)
                response.raise_for_status()
                data = response.json()
                
                if not data["markets"]:
                    break
                    
                batch = pd.DataFrame.from_dict(data["markets"])
                markets = pd.concat([markets, batch], ignore_index=True)
                
                cursor = data["cursor"]
                if not cursor:
                    break
                    
            except Exception as e:
                logging.error(f"Error fetching markets: {e}")
                break
        markets['full_title'] = markets.apply(
            lambda row: (
                f"{row['title']} {row['yes_sub_title']}"  # Use 'yes_sub_title' if 'subtitle' is empty, "::", or null
                if (row['subtitle'] == '::' or row['subtitle'] == '' or pd.isnull(row['subtitle'])) else
                row['title'] if row['subtitle'].lower() in row['title'].lower() else
                f"{row['title']} {row['subtitle']}"  # Otherwise, use 'title' and 'subtitle'
            ), axis=1
        )
        return markets
    
    def save_to_csv(self, df, filename="kalshi_markets.csv"):
        try:
            df.to_csv(filename, index=False)
            logging.info(f"Saved {len(df)} markets to {filename}")
        except Exception as e:
            logging.error(f"Error saving to CSV: {e}")


def main():
    logging.basicConfig(level=logging.INFO)
    polyMarketApi = PolyMarketAPI()
    kalshiApi = KalshiAPI(os.getenv("KALSHI_EMAIL"), os.getenv("KALSHI_PASSWORD"))
    
    logging.info("Fetching market data...")
    polyMarkets = polyMarketApi.get_markets()
    kalshiMarkets = kalshiApi.get_markets()
    
    polyMarketApi.save_to_csv(polyMarkets)
    kalshiApi.save_to_csv(kalshiMarkets)
    logging.info(f"Total markets saved: {len(polyMarkets) + len(kalshiMarkets)}")

if __name__ == "__main__":
    main()

  

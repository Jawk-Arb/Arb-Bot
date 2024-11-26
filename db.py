import os
import dotenv
from supabase import create_client, Client

dotenv.load_dotenv()

url: str = os.environ.get("SUPABASE_URL")
key: str = os.environ.get("SUPABASE_KEY")
supabase: Client = create_client(url, key)

NOT_FOUND = "NOT_FOUND"

def get_market_verification(kalshi_ticker, polymarket_id):
    """Get the verification status of a market"""
    response = supabase.table('markets').select('*').eq('kalshi_ticker', kalshi_ticker).eq('polymarket_id', polymarket_id).execute()
    
    if response.data:
        return response.data[0]['is_match']
    return NOT_FOUND
l
def insert_market_verification(kalshi_ticker, polymarket_id, kalshi_title, , poly_question, is_match):
    """Insert the verification status of a market"""
    supabase.table('markets').insert({'kalshi_ticker': kalshi_ticker, 'polymarket_id': polymarket_id, 'is_match': is_match, 'poly_question': poly_question, 'kalshi_title': kalshi_title}).execute()

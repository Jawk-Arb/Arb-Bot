import json
import requests
from datetime import datetime, date
import os
from py_clob_client.clob_types import OrderArgs, OrderType, ApiCreds, MarketOrderArgs
from py_clob_client.order_builder.constants import BUY
from py_clob_client.client import ClobClient
from py_clob_client.constants import POLYGON, AMOY
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor
import logging
from pathlib import Path

# Load environment variables at startup
load_dotenv()

def setup_logging():
    """Setup logging directories and formats"""
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    
    # Create separate log files for each exchange
    kalshi_handler = logging.FileHandler(f"logs/kalshi_orders_{datetime.now().strftime('%Y%m%d')}.log")
    poly_handler = logging.FileHandler(f"logs/polymarket_orders_{datetime.now().strftime('%Y%m%d')}.log")
    
    formatter = logging.Formatter('%(asctime)s - %(message)s')
    kalshi_handler.setFormatter(formatter)
    poly_handler.setFormatter(formatter)
    
    # Setup individual loggers
    kalshi_logger = logging.getLogger('kalshi')
    poly_logger = logging.getLogger('polymarket')
    
    kalshi_logger.addHandler(kalshi_handler)
    poly_logger.addHandler(poly_handler)
    
    kalshi_logger.setLevel(logging.INFO)
    poly_logger.setLevel(logging.INFO)
    
    return kalshi_logger, poly_logger

def execute_polymarket_order(polymarket_params):
    """
    Execute a Polymarket order with configurable parameters
    """
    logger = logging.getLogger('polymarket')

    client = polymarket_params['client']
    
    order_args = MarketOrderArgs(
        token_id=polymarket_params['token_id'],
        amount=polymarket_params['amount']
    )
    logger.info(f"Order Args: {order_args}")
    signed_order = client.create_market_order(order_args)
    resp = client.post_order(signed_order, orderType=OrderType.FOK)
    logger.info(f"Order Response: {json.dumps(resp, indent=2)}")
    return resp

def execute_kalshi_order(kalshi_params):
    """
    Execute a Kalshi order with configurable parameters
    """
    logger = logging.getLogger('kalshi')
    logger.info(f"Kalshi Params: {kalshi_params}")
    order_type = "market"

    try:
        url = "https://api.elections.kalshi.com/trade-api/v2/portfolio/orders"
        payload = {
            "action": "buy",
            "client_order_id": f"{kalshi_params['ticker']}_{datetime.now().strftime('%Y-%m-%d')}",
            "ticker": kalshi_params['ticker'],
            "count": int(kalshi_params['count']),
            "type": order_type,
            "side": kalshi_params['side']
        }
        headers = {
            "accept": "application/json",
            "content-type": "application/json",
            "Authorization": f"Bearer {kalshi_params['auth_token']}"
        }
        logger.info(f"Order Payload: {json.dumps(payload, indent=2)}")
        logger.info(f"Order Headers: {headers}")
        
        response = requests.post(url, json=payload, headers=headers).json()
        logger.info(f"Order Response: {json.dumps(response, indent=2)}")
        return response
    except Exception as e:
        logger.error(f"Order Failed: {str(e)}")
        return None

def execute_order(kalshi_params, polymarket_params):
    """Executes orders on both Kalshi and Polymarket in parallel using threads"""
    kalshi_logger, poly_logger = setup_logging()
    with ThreadPoolExecutor(max_workers=2) as executor:
        kalshi_future = executor.submit(execute_kalshi_order, kalshi_params)
        poly_future = executor.submit(execute_polymarket_order, polymarket_params)
        
        kalshi_order = kalshi_future.result()
        polymarket_order = poly_future.result()
        
    return kalshi_order, polymarket_order

def kalshi_auth() -> str:
    """Get Kalshi authentication token"""
    email = os.getenv("KALSHI_EMAIL")
    password = os.getenv("KALSHI_PASSWORD")
    url = "https://trading-api.kalshi.com/trade-api/v2/login"
    payload = {"email": email, "password": password}
    headers = {
        "accept": "application/json",
        "content-type": "application/json"
    }
    response = requests.post(url, json=payload, headers=headers).json()
    return response["token"]

def get_polymarket_client():
    host = os.getenv("POLYMARKET_HOST")
    key = os.getenv("POLYMARKET_KEY")
    funder = os.getenv("POLYMARKET_FUNDER")

    creds = ApiCreds(
        api_key=os.getenv("POLYMARKET_API_CREDS"),
        api_secret=os.getenv("POLYMARKET_API_SECRET"),
        api_passphrase=os.getenv("POLYMARKET_API_PASSPHRASE"),
    )
    chain_id = POLYGON
    client = ClobClient(host, key=key, chain_id=chain_id, funder=funder, signature_type=2, creds=creds)
    return client


if __name__ == "__main__":
    get_polymarket_client()
    
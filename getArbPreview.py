import pandas
import requests
import ast
import pandas as pd

from py_clob_client.client import ClobClient
from py_clob_client.clob_types import BookParams
from py_clob_client.clob_types import OrderArgs, OrderType
from py_clob_client.order_builder.constants import BUY
from py_clob_client.constants import POLYGON
from py_clob_client.clob_types import ApiCreds, MarketOrderArgs
from dotenv import load_dotenv
from py_clob_client.constants import AMOY

load_dotenv()

def calculate_arbitrage(kalshi_buy, kalshi_sell, polymarket_buy, polymarket_sell, stake):
    """
    Calculate arbitrage opportunities between two markets

    Args:
        kalshi_buy (float): Market 1 YES probability
        kalshi_sell (float): Market 1 NO probability
        polymarket_buy (float): Market 2 YES probability
        polymarket_sell (float): Market 2 NO probability
        stake (float): Total stake amount

    Returns:
        dict: Dictionary containing arbitrage calculations
    """

    # Find best opportunities
    best_yes = max(kalshi_buy, polymarket_buy)
    best_no = max(kalshi_sell, polymarket_sell)
    worst_yes = min(kalshi_buy, polymarket_buy)
    worst_no = min(kalshi_sell, polymarket_sell)

    # Record which market to use for buying and selling
    yes_market = "kalshi" if kalshi_buy == worst_yes else "polymarket"
    no_market = "kalshi" if kalshi_sell == worst_no else "polymarket"

    if yes_market == no_market:
        return 'No Arbitrage'  # No arbitrage opportunity
    
    # Check if arbitrage exists (YES price + NO price < 1)
    if worst_yes + worst_no >= 1:
        return 'No Arbitrage'  # No arbitrage opportunity

    yes_diff = abs(kalshi_buy - polymarket_buy)
    no_diff = abs(kalshi_sell - polymarket_sell)

    # Calculate optimal stakes - pro rata
    yes_stake = stake * worst_yes
    no_stake = stake * worst_no
    total_investment = yes_stake + no_stake

    # Adjust stakes proportionally if total doesn't match
    yes_stake = yes_stake * (stake / total_investment)
    no_stake = no_stake * (stake / total_investment)
    total_investment = yes_stake + no_stake

    # Calculate potential outcomes
    outcome_if_yes = (yes_stake * (1 / worst_yes)) - total_investment
    outcome_if_no = (no_stake * (1 / worst_no)) - total_investment

    min_profit = min(outcome_if_yes, outcome_if_no)
    max_profit = max(outcome_if_yes, outcome_if_no)

    return {
        "optimal_allocation": {
            "yes_stake": yes_stake,
            "no_stake": no_stake,
            "total_investment": total_investment,
        },
        "market_allocation": {
            "yes_market": yes_market,
            "no_market": no_market,
        },
        "outcomes": {
            "if_yes": outcome_if_yes,
            "if_no": outcome_if_no,
            "min_profit": min_profit,
            "max_profit": max_profit,
            "min_roi": (min_profit / total_investment) * 100 if total_investment > 0 else 0,
        },
    }

def get_current_prices(kalshi_auth_token, polymarket_client, kalshi_ticker, polymarket_id):

  #KALSHI UPDATED MARKET PRICES
  kalshi_generic_url = "https://api.elections.kalshi.com/trade-api/v2/markets/"

  headers = {
      "accept": "application/json",
      "Authorization": f"Bearer {kalshi_auth_token}",
  }
  print(headers)


  kalshi_url = kalshi_generic_url+kalshi_ticker
  response = requests.get(url = kalshi_url, headers=headers)

  kalshi_yes_ask = float(response.json()['market']['yes_ask'])/100
  kalshi_no_ask = float(response.json()['market']['no_ask'])/100


  #############################
  #GET TOKEN IDS FOR BUY & SELL
  #############################
  polymarket_generic_url = "https://gamma-api.polymarket.com/markets/"
  polymarket_url = polymarket_generic_url+str(polymarket_id)
  response = requests.get(url = polymarket_url)

  tokens = ast.literal_eval(response.json()['clobTokenIds'])
  polymarket_yes_token = tokens[0]
  polymarket_no_token = tokens[1]


  resp = polymarket_client.get_prices(
      params=[
          BookParams(
              token_id=polymarket_yes_token,
              side="SELL",
          ),
          BookParams(
              token_id=polymarket_no_token,
              side="SELL",
          ),
      ]
  )

  polymarket_yes_ask = float(resp[polymarket_yes_token]['SELL'] if resp else 0)
  polymarket_no_ask = float(resp[polymarket_no_token]['SELL'] if resp else 0)

  return kalshi_yes_ask, kalshi_no_ask, polymarket_yes_ask, polymarket_no_ask, polymarket_yes_token, polymarket_no_token


#get kalshi auth token
url = "https://trading-api.kalshi.com/trade-api/v2/login"

payload = {
    "email": os.getenv("KALSHI_EMAIL"),
    "password": os.getenv("KALSHI_PASSWORD")
}
headers = {
    "accept": "application/json",
    "content-type": "application/json"
}

response = requests.post(url, json=payload, headers=headers).json()
kalshi_auth_token = response["token"]




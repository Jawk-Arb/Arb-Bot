import json
import requests
from datetime import datetime
import os
from datetime import date
from py_clob_client.clob_types import OrderArgs, OrderType
from py_clob_client.order_builder.constants import BUY
from py_clob_client.client import ClobClient
from py_clob_client.constants import POLYGON
from py_clob_client.clob_types import ApiCreds, MarketOrderArgs
from dotenv import load_dotenv
from py_clob_client.constants import AMOY 
from dotenv import load_dotenv

load_dotenv()

def main():
    host = "https://clob.polymarket.com"
    key = os.getenv("POLYMARKET_KEY")
    funder = os.getenv("POLYMARKET_FUNDER")
    chain_id = POLYGON


    # Create CLOB client and get/set API credentials
    # Need Signature Type 2 for Trading
    client = ClobClient(host, key=key, chain_id=chain_id, funder=funder, signature_type=2)

	

    creds = client.create_or_derive_api_creds()
    client.set_api_creds(creds)
    order_args = MarketOrderArgs(
        token_id="29248242988957024957749145407613210275607073690234607747607028828964749034344", 
        amount=1
    )
    signed_order = client.create_market_order(order_args)
    resp = client.post_order(signed_order, orderType=OrderType.FOK)
    print(resp)
    print("Done!")

if __name__ == "__main__":
    main()
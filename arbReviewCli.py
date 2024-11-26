from rich.console import Console
from rich.panel import Panel
from simple_term_menu import TerminalMenu
import pandas as pd
from curl_cffi import requests
import json
from typing import Dict, Any
from getArbPreview import get_current_prices, calculate_arbitrage
from placeOrder import kalshi_auth, get_polymarket_client, execute_order
from db import supabase, NOT_FOUND, get_market_verification, insert_market_verification


global result

console = Console()

kalshi_auth_token = kalshi_auth()
polymarket_client = get_polymarket_client()

def load_arb_data(file_path='similar_markets.csv') -> pd.DataFrame:
    """Load and parse arbitrage data from CSV"""
    return pd.read_csv(file_path)

def format_arb_preview(arb_preview) -> Dict[str, Any]:
    """Parse and format the Arb Preview"""
    try:
        return {
            'optimal_allocation': f"Yes: ${arb_preview['optimal_allocation']['yes_stake']:.2f}, No: ${arb_preview['optimal_allocation']['no_stake']:.2f}",
            'market_allocation': f"Yes: {arb_preview['market_allocation']['yes_market']}, No: {arb_preview['market_allocation']['no_market']}",
            'min_roi': f"{arb_preview['outcomes']['min_roi']:.2f}%"
        }
    except:
        return {'optimal_allocation': 'N/A', 'market_allocation': 'N/A', 'min_roi': 'N/A'}

def display_opportunity(arb_opportunity: object, row: object, prices: object) -> None:
    """Display single arbitrage opportunity"""
    arb_data = format_arb_preview(arb_opportunity)

    
    content = f"""
[bold]Polymarket Question:[/bold] {row['poly_question']}
[bold]Kalshi Title:[/bold] {row['kalshi_title']}

[bold]Prices:[/bold]
Polymarket: Yes=${prices['polymarket_yes_ask']:.2f} No=${prices['polymarket_no_ask']:.2f}
Kalshi:     Yes=${prices['kalshi_yes_ask']:.2f} No=${prices['kalshi_no_ask']:.2f}

[bold]Arbitrage Details:[/bold]
Allocation: {arb_data['optimal_allocation']}
Markets: {arb_data['market_allocation']}
Min ROI: {arb_data['min_roi']}
"""
    console.print(Panel(content, title="Arbitrage Opportunity", expand=False))

def review_market_and_arb(row):
    console.clear()
    # First panel: Market comparison
    comparison = f"""
[bold]Polymarket Question:[/bold] {row['poly_question']}
[bold]Kalshi Title:[/bold] {row['kalshi_title']}
    """
    console.print(Panel(comparison, title="Market Comparison", expand=False))
    
    options = ["Markets Match (Show Arb)", "Markets Different", "Skip", "Quit"]
    terminal_menu = TerminalMenu(options, title="Are these markets equivalent?")
    # Get the verification status of the market
    market_verification_result = get_market_verification(row['kalshi_id'], row['poly_id'])
    choice = 1
    if market_verification_result == True:
        choice = 0
    elif market_verification_result == False:
        return 1
    # Market not found case
    else:
        choice = terminal_menu.show()
    
    if choice == 0:  # Markets Match
        if market_verification_result == NOT_FOUND:
            insert_market_verification(row['kalshi_id'], row['poly_id'], row['kalshi_title'], row['poly_question'], True)
        console.clear()
        kalshi_yes_ask, kalshi_no_ask, polymarket_yes_ask, polymarket_no_ask, polymarket_yes_token, polymarket_no_token = get_current_prices(
            kalshi_auth_token, polymarket_client, row['kalshi_id'], row['poly_id']
        )
        prices = {
            'kalshi_yes_ask': kalshi_yes_ask,
            'kalshi_no_ask': kalshi_no_ask,
            'polymarket_yes_ask': polymarket_yes_ask,
            'polymarket_no_ask': polymarket_no_ask,
            'polymarket_yes_token': polymarket_yes_token,
            'polymarket_no_token': polymarket_no_token
        }
        if (kalshi_yes_ask > 0 and kalshi_no_ask > 0 and 
            polymarket_yes_ask > 0 and polymarket_no_ask > 0):
            # Last Parameter is the stake size (in dollars)
            arb_opportunity = calculate_arbitrage(
                kalshi_yes_ask, kalshi_no_ask, 
                polymarket_yes_ask, polymarket_no_ask, 10
            )
            if arb_opportunity == 'No Arbitrage':
                return "pass"
            display_opportunity(arb_opportunity, row, prices)
            row['arb_opportunity'] = arb_opportunity
            row['prices'] = prices
        else:
            return "pass"
        
        options = ["Execute Trade", "Pass"]
        terminal_menu = TerminalMenu(options, title="Select Action:")
        action = terminal_menu.show()
        return "execute" if action == 0 else "pass"
    else:
        if market_verification_result == NOT_FOUND:
            insert_market_verification(row['kalshi_id'], row['poly_id'], row['kalshi_title'], row['poly_question'], False)
    
    return {
        1: "different",
        2: "skip", 
        3: "quit"
    }.get(choice)

def review_arb_opportunities():
    """Process opportunities one by one with menu selection"""
    df = load_arb_data()
    verified_markets = []
    
    for index, row in df.iterrows():

            
        result = review_market_and_arb(row)
        
        if result == "quit":
            break
        elif result in ["skip", "different", "pass"]:
            continue
        elif result == "execute":
            kalshi_params, polymarket_params = prepare_orders(row)
            execute_order(kalshi_params, polymarket_params)
            verified_markets.append(row)
            console.print("[green]Trade executed![/green]")
    
    return verified_markets

def prepare_orders(row):
    """Prepare orders for execution"""
    
    arb_opportunity = row['arb_opportunity']
    prices = row['prices']

    kalshi_params = {
        'auth_token': kalshi_auth_token,
        'ticker': row['kalshi_id'],
        'count': (arb_opportunity['optimal_allocation']['yes_stake'] / prices['kalshi_yes_ask']) if arb_opportunity['market_allocation']['yes_market'] == 'kalshi' else (arb_opportunity['optimal_allocation']['no_stake'] / prices['kalshi_no_ask']),
        'side': 'yes' if arb_opportunity['market_allocation']['yes_market'] == 'kalshi' else 'no'
    }
    polymarket_params = {
        'token_id': prices['polymarket_yes_token'] if arb_opportunity['market_allocation']['yes_market'] == 'polymarket' else prices['polymarket_no_token'],
        'amount': arb_opportunity['optimal_allocation']['yes_stake'] if arb_opportunity['market_allocation']['yes_market'] == 'polymarket' else arb_opportunity['optimal_allocation']['no_stake'],
        'client': polymarket_client
    }
    return kalshi_params, polymarket_params
    

def main():
    console.print("[bold blue]Arbitrage Opportunity Review[/bold blue]")
    verified_markets = review_arb_opportunities()
    
    if verified_markets:
        df = pd.DataFrame(verified_markets)
        df.to_csv('verified_markets.csv', index=False)
        
        console.print("\n[bold green]Accepted Opportunities:[/bold green]")
        for row in verified_markets:
            console.print(f"- {row['poly_question']}")
    
    console.print(f"\nProcessed {len(verified_markets)} opportunities.")

if __name__ == "__main__":
    main() 
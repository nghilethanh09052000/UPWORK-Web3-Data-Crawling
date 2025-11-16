import json
import requests
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from itertools import cycle
from threading import Lock

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    handlers=[
        logging.FileHandler('coingecko.log'),
        logging.StreamHandler()
    ]
)

# Thread-safe file lock
file_lock = Lock()

# Load proxies
def load_proxies():
    """Load proxies from file"""
    with open('project/proxyscrape_premium_http_proxies.txt', 'r') as f:
        proxies = [line.strip() for line in f if line.strip()]
    logging.info(f"Loaded {len(proxies)} proxies")
    return cycle(proxies)

# Global proxy rotator
proxy_rotator = load_proxies()


def save_result_to_json(result, output_file='coingecko_extracted.json'):
    """Save a single result to JSON file (thread-safe)"""
    with file_lock:
        try:
            with open(output_file, 'r') as f:
                data = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            data = []
        
        data.append(result)
        
        with open(output_file, 'w') as f:
            json.dump(data, f, indent=2)


def call_coingecko_api(coin_id, proxy):
    """Call CoinGecko API with proxy and return (data, status)"""
    if not coin_id:
        return None, "no_id"
    
    url = f"https://api.coingecko.com/api/v3/coins/{coin_id}"
    proxies = {
        'http': f'http://{proxy}',
        'https': f'http://{proxy}'
    }
    
    try:
        response = requests.get(url, proxies=proxies, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            links = data.get("links", {})
            
            # Extract github repos
            github_repos = links.get("repos_url", {}).get("github", [])
            
            # Extract twitter
            twitter_screen_name = links.get("twitter_screen_name")
            twitter_url = f"https://x.com/{twitter_screen_name}" if twitter_screen_name else ""
            
            return {
                "name": data.get("name"),
                "symbol": data.get("symbol", "").upper(),
                "github": github_repos,
                "twitter": twitter_url,
                "website": links.get("homepage", [None])[0] or "",
                "coingecko_id": coin_id
            }, "success"
        elif response.status_code == 429:
            return None, "rate_limit"
        elif response.status_code == 404:
            return None, "not_found"
        else:
            return None, f"error_{response.status_code}"
            
    except Exception as e:
        return None, f"exception"


def process_chain(chain, index, total):
    """Process a single chain"""
    chain_name = chain.get('name', 'Unknown')
    chain_symbol = chain.get('symbol', '')
    
    # Convert name to CoinGecko ID format (lowercase, replace spaces with dashes)
    coin_id = chain_name.lower().replace(' ', '-')
    
    proxy = next(proxy_rotator)
    logging.info(f"[{index}/{total}] {chain_name} ({chain_symbol}) - Calling: {coin_id}")
    
    data, status = call_coingecko_api(coin_id, proxy)
    
    if status == "success":
        data['chain_name'] = chain_name
        data['chain_symbol'] = chain_symbol
        save_result_to_json(data)
        logging.info(f"  ✓ SUCCESS - Saved to JSON")
        return data
    else:
        logging.info(f"  ✗ {status}")
        return None


def extract_data(max_workers=10):
    """Main function to extract data from defilama_chains.json using parallel processing"""
    
    # Read defilama chains
    logging.info("Reading defilama_chains.json...")
    with open('/Users/nghilethanh/Project/UPWORK-Scrape-Interactive-Chart-Data /defilama_chains.json', 'r') as f:
        data = json.load(f)
    
    # Extract chains from props.pageProps.chains
    chains = data['props']['pageProps']['chains']
    
    logging.info(f"Found {len(chains)} chains")
    logging.info(f"Starting parallel extraction with {max_workers} workers...")
    
    # Initialize empty JSON file
    with open('coingecko_extracted.json', 'w') as f:
        json.dump([], f)
    
    success_count = 0
    
    # Use ThreadPoolExecutor for parallel processing
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_chain = {
            executor.submit(process_chain, chain, i+1, len(chains)): chain 
            for i, chain in enumerate(chains)
        }
        
        # Process completed tasks
        for future in as_completed(future_to_chain):
            try:
                result = future.result()
                if result:
                    success_count += 1
            except Exception as e:
                logging.error(f"Error: {e}")
    
    logging.info(f"\n{'='*60}")
    logging.info(f"✓ Done!")
    logging.info(f"✓ Successfully extracted: {success_count} chains")
    logging.info(f"✓ Saved to: coingecko_extracted.json")
    logging.info(f"{'='*60}")


if __name__ == '__main__':
    import sys
    
    # Allow custom number of workers
    max_workers = int(sys.argv[1]) if len(sys.argv) > 1 else 10
    
    logging.info(f"Using {max_workers} parallel workers")
    extract_data(max_workers=max_workers)

import scrapy
import json
import os
from datetime import datetime


class DefillamaTvlSpider(scrapy.Spider):
    name = "defillama_tvl"
    
    custom_settings = {
        'USER_AGENT': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36 Edg/142.0.0.0',
        'CONCURRENT_REQUESTS': 50,  # Increase concurrency for speed
        #'DOWNLOAD_DELAY': 0.1,  # Small delay to avoid overwhelming the server
        #'RETRY_TIMES': 3,
    }
    
    def start_requests(self):
        # Read protocols from JSON file
        protocols_file = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'protocols_list.json')
        
        try:
            with open(protocols_file, 'r', encoding='utf-8') as f:
                protocols = json.load(f)
            
            self.logger.info(f"Loaded {len(protocols)} protocols from {protocols_file}")
            
            # Generate requests for each protocol
            for protocol in protocols:
                slug = protocol.get('slug')
                name = protocol.get('name', slug)
                
                if slug:
                    url = f'https://api.llama.fi/updatedProtocol/{slug}'
                    yield scrapy.Request(
                        url=url,
                        callback=self.parse_protocol,
                        headers={
                            'referer': 'https://defillama.com/',
                            'sec-ch-ua': '"Chromium";v="142", "Microsoft Edge";v="142", "Not_A Brand";v="99"',
                            'sec-ch-ua-mobile': '?0',
                            'sec-ch-ua-platform': '"macOS"',
                            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36 Edg/142.0.0.0'
                        },
                        meta={'protocol_name': name, 'slug': slug}
                    )
                else:
                    self.logger.warning(f"Skipping protocol without slug: {name}")
        
        except FileNotFoundError:
            self.logger.error(f"Protocols file not found: {protocols_file}")
            self.logger.error("Please run: scrapy crawl extract_protocols -o protocols_list.json")
        except json.JSONDecodeError as e:
            self.logger.error(f"Error parsing protocols JSON: {e}")
    
    def parse_protocol(self, response):
        """Parse protocol TVL data"""
        protocol_name = response.meta.get('protocol_name', 'Unknown')
        slug = response.meta.get('slug', 'unknown')
        
        try:
            data = json.loads(response.text)
            protocol_name = data.get('name', protocol_name)
            chain_tvls = data.get('chainTvls', {})
            chains_list = data.get('chains', [])
            
            rows_count = 0
            
            # Check if we have chainTvls data
            if chain_tvls:
                # Process each chain
                for chain_name, chain_data in chain_tvls.items():
                    # Skip keys that end with '-borrowed' or other non-chain metrics
                    if chain_name.endswith('-borrowed') or chain_name.endswith('-staking') or chain_name.endswith('-pool2'):
                        continue
                    
                    # Skip special keys
                    if chain_name in ['borrowed', 'staking', 'pool2', 'vesting', 'treasury']:
                        continue
                    
                    # Get TVL data
                    tvl_data = chain_data.get('tvl', [])
                    
                    if tvl_data:
                        for entry in tvl_data:
                            # Convert Unix timestamp to date
                            timestamp = entry.get('date', 0)
                            date_str = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d')
                            tvl_usd = entry.get('totalLiquidityUSD', 0)
                            
                            rows_count += 1
                            yield {
                                'protocol_name': protocol_name,
                                'date': date_str,
                                'chain': chain_name,
                                'tvl_usd': tvl_usd
                            }
                
                if rows_count > 0:
                    chains_count = len([k for k in chain_tvls.keys() if not k.endswith('-borrowed') and not k.endswith('-staking') and not k.endswith('-pool2') and k not in ['borrowed', 'staking', 'pool2', 'vesting', 'treasury']])
                    self.logger.info(f"✅ {protocol_name} - {rows_count} rows across {chains_count} chains")
                else:
                    # If chainTvls exists but no valid data, try to get main TVL
                    tvl_data = data.get('tvl', [])
                    
                    if tvl_data and chains_list:
                        # Use the first chain as the hosting chain for single-chain protocols
                        chain_name = chains_list[0] if len(chains_list) == 1 else 'Multiple'
                        
                        for entry in tvl_data:
                            timestamp = entry.get('date', 0)
                            date_str = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d')
                            tvl_usd = entry.get('totalLiquidityUSD', 0)
                            
                            rows_count += 1
                            yield {
                                'protocol_name': protocol_name,
                                'date': date_str,
                                'chain': chain_name,
                                'tvl_usd': tvl_usd
                            }
                        
                        self.logger.info(f"✅ {protocol_name} (single-chain: {chain_name}) - {rows_count} rows")
                    else:
                        self.logger.warning(f"⚠️  {protocol_name} - No valid TVL data")
            else:
                # No chainTvls, try to get main TVL
                tvl_data = data.get('tvl', [])
                
                if tvl_data:
                    # Determine chain name
                    if chains_list:
                        chain_name = chains_list[0] if len(chains_list) == 1 else 'Multiple'
                    else:
                        chain_name = 'Unknown'
                    
                    for entry in tvl_data:
                        timestamp = entry.get('date', 0)
                        date_str = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d')
                        tvl_usd = entry.get('totalLiquidityUSD', 0)
                        
                        rows_count += 1
                        yield {
                            'protocol_name': protocol_name,
                            'date': date_str,
                            'chain': chain_name,
                            'tvl_usd': tvl_usd
                        }
                    
                    self.logger.info(f"✅ {protocol_name} (chain: {chain_name}) - {rows_count} rows")
                else:
                    self.logger.warning(f"⚠️  {protocol_name} - No TVL data")
        
        except json.JSONDecodeError as e:
            self.logger.error(f"❌ {protocol_name} - JSON decode error: {e}")
        except Exception as e:
            self.logger.error(f"❌ {protocol_name} - Error: {e}")


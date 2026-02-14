import scrapy
import json
import os
import re
import gzip
from datetime import datetime
from calendar import monthrange
from collections import defaultdict


class DefillamaMetricsSpider(scrapy.Spider):
    name = "defillama_metrics"
    
    custom_settings = {
        'USER_AGENT': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36 Edg/142.0.0.0',
        'CONCURRENT_REQUESTS': 200,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 200,
        'DOWNLOAD_DELAY': 0,
        'RANDOMIZE_DOWNLOAD_DELAY': False,
        'RETRY_TIMES': 3,
        'RETRY_HTTP_CODES': [500, 502, 503, 504, 408, 429],
        'DOWNLOAD_TIMEOUT': 30,
        'AUTOTHROTTLE_ENABLED': False,
        'FEEDS': {
            '../defillama_metrics_2.csv': {
                'format': 'csv',
                'overwrite': True,
                'fields': [
                    'protocol_name', 'timestamp', 'date',
                    'tvl', 'mcap', 'tokenprice', 'tokenvolume', 'fdv', 'tokenliquidity',
                    'fees', 'revenue', 'holdersrevenue', 'dexvolume',
                    'unlocks', 'unlockstokens', 'incentives',
                    'totalproposals', 'successfulproposals', 'maxvotes',
                    'treasury'
                ]
            }
        }
    }
        
    def start_requests(self):
        # Read protocols from JSON file (JSON array format)
        protocols_file = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
            'protocols_list.json'
        )
        
        try:
            with open(protocols_file, 'r', encoding='utf-8') as f:
                protocols = json.load(f)
            
            # Handle both array and single object formats
            if not isinstance(protocols, list):
                protocols = [protocols]
            
            self.logger.info(f"Loaded {len(protocols)} protocols from {protocols_file}")
            self.total_protocols = len(protocols)
            
            # Generate requests for each protocol's page to extract __NEXT_DATA__
            for protocol in protocols:
                # Extract slug directly from the protocol object
                slug = protocol.get('slug')
                name = protocol.get('name', slug)
                
                if slug:
                    url = f'https://defillama.com/protocol/{slug}'
                    yield scrapy.Request(
                        url=url,
                        callback=self.parse_protocol_page,
                        headers={
                            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
                            'accept-language': 'en-US,en;q=0.9',
                            'sec-ch-ua': '"Chromium";v="142", "Microsoft Edge";v="142", "Not_A Brand";v="99"',
                            'sec-ch-ua-mobile': '?0',
                            'sec-ch-ua-platform': '"macOS"',
                            'sec-fetch-dest': 'document',
                            'sec-fetch-mode': 'navigate',
                            'sec-fetch-site': 'none',
                            'upgrade-insecure-requests': '1',
                        },
                        meta={
                            'protocol_name': name,
                            'slug': slug,
                            'dont_redirect': True,
                            'handle_httpstatus_list': [200, 404, 500]
                        }
                    )
                else:
                    self.logger.warning(f"Skipping protocol without slug: {name}")
        
        except FileNotFoundError:
            self.logger.error(f"Protocols file not found: {protocols_file}")
        except json.JSONDecodeError as e:
            self.logger.error(f"Error parsing protocols JSON: {e}")
        except Exception as e:
            self.logger.error(f"Error reading protocols file: {e}")
    
    def extract_slug_from_logo(self, logo_url):
        """Extract slug from logo URL"""
        if not logo_url:
            return None
        return logo_url.rstrip('/').split('/')[-1]
    
    def parse_protocol_page(self, response):
        """Extract __NEXT_DATA__ from protocol page and parse chart data"""
        protocol_name = response.meta['protocol_name']
        slug = response.meta['slug']
        
        if response.status != 200:
            self.logger.warning(f"❌ {slug}: HTTP {response.status}")
            return
        
        try:
            # Extract __NEXT_DATA__ JSON from the page
            next_data_match = re.search(r'<script id="__NEXT_DATA__" type="application/json">({.*?})</script>', response.text, re.DOTALL)
            
            if not next_data_match:
                self.logger.error(f"❌ {slug}: Could not find __NEXT_DATA__ in page")
                return
            
            next_data = json.loads(next_data_match.group(1))
            page_props = next_data.get('props', {}).get('pageProps', {})
            
            if not page_props:
                self.logger.error(f"❌ {slug}: No pageProps in __NEXT_DATA__")
                return
            
            # Extract TVL chart data and store in meta for later merging
            tvl_chart_data = page_props.get('tvlChartData', [])
            
            # Store TVL data indexed by timestamp
            tvl_by_timestamp = {}
            for entry in tvl_chart_data:
                if isinstance(entry, list) and len(entry) >= 2:
                    timestamp = str(entry[0])
                    tvl_by_timestamp[timestamp] = entry[1]
            
            # Extract protocol ID for historical liquidity API
            protocol_id = page_props.get('id')
            
            # Extract gecko_id from token data for CoinGecko API
            token_data = page_props.get('token', {})
            gecko_id = token_data.get('gecko_id')
            
            # Now fetch additional metrics via API calls
            meta = {
                'protocol_name': protocol_name,
                'slug': slug,
                'tvl_data': tvl_by_timestamp,
                'protocol_id': protocol_id,
                'gecko_id': gecko_id
            }
            
            # 1. Fetch CoinGecko chart (prices, mcap, volumes)
            # Use gecko_id if available, otherwise fallback to slug
            cg_identifier = gecko_id if gecko_id else slug
            cg_url = f'https://fe-cache.llama.fi/cgchart/{cg_identifier}?fullChart=true'
            yield scrapy.Request(
                url=cg_url,
                callback=self.parse_coingecko_chart,
                meta=meta,
                dont_filter=True,
                errback=lambda f: self.fetch_fees_chart(f.request.meta)
            )
        
        except json.JSONDecodeError as e:
            self.logger.error(f"❌ {slug}: Error parsing __NEXT_DATA__ JSON: {e}")
        except Exception as e:
            self.logger.error(f"❌ {slug}: Error processing page: {e}")
    
    def parse_coingecko_chart(self, response):
        """Parse CoinGecko chart data (prices, mcap, volumes)"""
        slug = response.meta['slug']
        
        try:
            if response.status == 200:
                data = json.loads(response.text)
                cg_data = data.get('data', {})
                
                # Extract max_supply from coinData
                coin_data = cg_data.get('coinData', {})
                market_data = coin_data.get('market_data', {})
                max_supply = market_data.get('max_supply')
                if not max_supply:
                    max_supply = market_data.get("circulating_supply")
                
                # Extract prices, market_caps, total_volumes
                prices = cg_data.get('prices', [])
                market_caps = cg_data.get('market_caps', [])
                volumes = cg_data.get('total_volumes', [])
                
                # Store in meta indexed by date (converted from milliseconds)
                price_by_date = {}
                for entry in prices:
                    if len(entry) >= 2:
                        # Convert milliseconds to seconds and then to date
                        timestamp_ms = entry[0]
                        timestamp = int(timestamp_ms / 1000)
                        date_str = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d')
                        if date_str not in price_by_date:
                            price_by_date[date_str] = {}
                        price_by_date[date_str]['tokenprice'] = entry[1]
                        price_by_date[date_str]['timestamp'] = str(timestamp)
                
                mcap_by_date = {}
                for entry in market_caps:
                    if len(entry) >= 2:
                        timestamp_ms = entry[0]
                        timestamp = int(timestamp_ms / 1000)
                        date_str = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d')
                        if date_str not in mcap_by_date:
                            mcap_by_date[date_str] = {}
                        mcap_by_date[date_str]['mcap'] = entry[1]
                
                volume_by_date = {}
                for entry in volumes:
                    if len(entry) >= 2:
                        timestamp_ms = entry[0]
                        timestamp = int(timestamp_ms / 1000)
                        date_str = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d')
                        if date_str not in volume_by_date:
                            volume_by_date[date_str] = {}
                        volume_by_date[date_str]['tokenvolume'] = entry[1]
                
                # Store max_supply in meta for FDV calculation in merge function
                response.meta['max_supply'] = max_supply
                
                response.meta['price_data'] = price_by_date
                response.meta['mcap_data'] = mcap_by_date
                response.meta['volume_data'] = volume_by_date
                
                if max_supply:
                    self.logger.debug(f"✅ {slug}: CG data - {len(prices)} prices, {len(market_caps)} mcaps, {len(volumes)} volumes, max_supply: {max_supply}")
                else:
                    self.logger.debug(f"✅ {slug}: CG data - {len(prices)} prices, {len(market_caps)} mcaps, {len(volumes)} volumes (no max_supply)")
        
        except Exception as e:
            self.logger.warning(f"⚠️  {slug}: Error parsing CoinGecko data: {e}")
        
        # Check if protocol has ID and fetch historical liquidity
        protocol_id = response.meta.get('protocol_id')
        if protocol_id:
            return self.fetch_historical_liquidity(response.meta, protocol_id)
        
        # Continue to fees chart
        return self.fetch_fees_chart(response.meta)
    
    def fetch_historical_liquidity(self, meta, protocol_id):
        """Fetch historical liquidity data"""
        slug = meta['slug']
        liquidity_url = f'https://api.llama.fi/historicalLiquidity/{protocol_id}'
        
        return scrapy.Request(
            url=liquidity_url,
            callback=self.parse_historical_liquidity,
            meta=meta,
            dont_filter=True,
            errback=lambda f: self.fetch_fees_chart(f.request.meta)
        )
    
    def parse_historical_liquidity(self, response):
        """Parse historical liquidity data"""
        slug = response.meta['slug']
        
        try:
            if response.status == 200:
                data = json.loads(response.text)
                
                # Data format: [[timestamp, liquidity_value], ...]
                liquidity_by_date = {}
                for entry in data:
                    if isinstance(entry, list) and len(entry) >= 2:
                        timestamp = entry[0]
                        liquidity_value = entry[1]
                        
                        # Convert timestamp to date string
                        try:
                            date_str = datetime.fromtimestamp(int(timestamp)).strftime('%Y-%m-%d')
                            liquidity_by_date[date_str] = {
                                'tokenliquidity': liquidity_value,
                                'timestamp': str(timestamp)
                            }
                        except Exception as e:
                            self.logger.debug(f"⚠️  {slug}: Error converting timestamp {timestamp}: {e}")
                            continue
                
                response.meta['liquidity_data'] = liquidity_by_date
                self.logger.debug(f"✅ {slug}: Historical liquidity data - {len(liquidity_by_date)} records")
            else:
                self.logger.debug(f"⚠️  {slug}: Historical liquidity API returned status {response.status}")
                response.meta['liquidity_data'] = {}
        
        except json.JSONDecodeError as e:
            self.logger.warning(f"⚠️  {slug}: Error parsing historical liquidity JSON: {e}")
            response.meta['liquidity_data'] = {}
        except Exception as e:
            self.logger.warning(f"⚠️  {slug}: Error parsing historical liquidity data: {e}")
            response.meta['liquidity_data'] = {}
        
        # Continue to fees chart
        return self.fetch_fees_chart(response.meta)
    
    def fetch_fees_chart(self, meta):
        """Fetch fees chart"""
        slug = meta['slug']
        fees_url = f'https://api.llama.fi/v2/chart/fees/protocol/{slug}'
        
        return scrapy.Request(
            url=fees_url,
            callback=self.parse_fees_chart,
            meta=meta,
            dont_filter=True,
            errback=lambda f: self.fetch_revenue_chart(f.request.meta)
        )
    
    def parse_fees_chart(self, response):
        """Parse fees chart data"""
        slug = response.meta['slug']
        
        try:
            if response.status == 200:
                data = json.loads(response.text)
                
                fees_by_date = {}
                for entry in data:
                    if len(entry) >= 2:
                        timestamp = str(entry[0])
                        date_str = datetime.fromtimestamp(int(timestamp)).strftime('%Y-%m-%d')
                        fees_by_date[date_str] = {'fees': entry[1], 'timestamp': timestamp}
                
                response.meta['fees_data'] = fees_by_date
                self.logger.debug(f"✅ {slug}: Fees data - {len(fees_by_date)} records")
        
        except Exception as e:
            self.logger.warning(f"⚠️  {slug}: Error parsing fees data: {e}")
        
        # Continue to revenue chart
        return self.fetch_revenue_chart(response.meta)
    
    def fetch_revenue_chart(self, meta):
        """Fetch revenue chart"""
        slug = meta['slug']
        revenue_url = f'https://api.llama.fi/v2/chart/fees/protocol/{slug}?dataType=dailyRevenue'
        
        return scrapy.Request(
            url=revenue_url,
            callback=self.parse_revenue_chart,
            meta=meta,
            dont_filter=True,
            errback=lambda f: self.fetch_holders_revenue_chart(f.request.meta)
        )
    
    def parse_revenue_chart(self, response):
        """Parse revenue chart data"""
        slug = response.meta['slug']
        
        try:
            if response.status == 200:
                data = json.loads(response.text)
                
                revenue_by_date = {}
                for entry in data:
                    if len(entry) >= 2:
                        timestamp = str(entry[0])
                        date_str = datetime.fromtimestamp(int(timestamp)).strftime('%Y-%m-%d')
                        revenue_by_date[date_str] = {'revenue': entry[1], 'timestamp': timestamp}
                
                response.meta['revenue_data'] = revenue_by_date
                self.logger.debug(f"✅ {slug}: Revenue data - {len(revenue_by_date)} records")
        
        except Exception as e:
            self.logger.warning(f"⚠️  {slug}: Error parsing revenue data: {e}")
        
        # Continue to holders revenue chart
        return self.fetch_holders_revenue_chart(response.meta)
    
    def fetch_holders_revenue_chart(self, meta):
        """Fetch holders revenue chart"""
        slug = meta['slug']
        holders_revenue_url = f'https://api.llama.fi/v2/chart/fees/protocol/{slug}?dataType=dailyHoldersRevenue'
        
        return scrapy.Request(
            url=holders_revenue_url,
            callback=self.parse_holders_revenue_chart,
            meta=meta,
            dont_filter=True,
            errback=lambda f: self.merge_and_yield(f.request.meta)
        )
    
    def parse_holders_revenue_chart(self, response):
        """Parse holders revenue chart data"""
        slug = response.meta['slug']
        
        try:
            if response.status == 200:
                data = json.loads(response.text)
                
                holders_revenue_by_date = {}
                for entry in data:
                    if len(entry) >= 2:
                        timestamp = str(entry[0])
                        date_str = datetime.fromtimestamp(int(timestamp)).strftime('%Y-%m-%d')
                        holders_revenue_by_date[date_str] = {'holdersrevenue': entry[1], 'timestamp': timestamp}
                
                response.meta['holders_revenue_data'] = holders_revenue_by_date
                self.logger.debug(f"✅ {slug}: Holders revenue data - {len(holders_revenue_by_date)} records")
        
        except Exception as e:
            self.logger.warning(f"⚠️  {slug}: Error parsing holders revenue data: {e}")
        
        # Continue to DEX volume chart
        return self.fetch_dex_volume_chart(response.meta)
    
    def fetch_dex_volume_chart(self, meta):
        """Fetch DEX volume chart"""
        slug = meta['slug']
        dex_volume_url = f'https://api.llama.fi/v2/chart/dexs/protocol/{slug}'
        
        return scrapy.Request(
            url=dex_volume_url,
            callback=self.parse_dex_volume_chart,
            meta=meta,
            dont_filter=True,
            errback=lambda f: self.fetch_treasury_chart(f.request.meta)
        )
    
    def parse_dex_volume_chart(self, response):
        """Parse DEX volume chart data"""
        slug = response.meta['slug']
        
        try:
            if response.status == 200:
                data = json.loads(response.text)
                
                # Data format: [[timestamp, volume_value], ...]
                dex_volume_by_date = {}
                for entry in data:
                    if isinstance(entry, list) and len(entry) >= 2:
                        timestamp = entry[0]
                        volume_value = entry[1]
                        
                        # Convert timestamp to date string
                        try:
                            date_str = datetime.fromtimestamp(int(timestamp)).strftime('%Y-%m-%d')
                            dex_volume_by_date[date_str] = {
                                'dexvolume': volume_value,
                                'timestamp': str(timestamp)
                            }
                        except Exception as e:
                            self.logger.debug(f"⚠️  {slug}: Error converting timestamp {timestamp}: {e}")
                            continue
                
                response.meta['dex_volume_data'] = dex_volume_by_date
                self.logger.debug(f"✅ {slug}: DEX volume data - {len(dex_volume_by_date)} records")
            else:
                self.logger.debug(f"⚠️  {slug}: DEX volume API returned status {response.status}")
                response.meta['dex_volume_data'] = {}
        
        except json.JSONDecodeError as e:
            self.logger.warning(f"⚠️  {slug}: Error parsing DEX volume JSON: {e}")
            response.meta['dex_volume_data'] = {}
        except Exception as e:
            self.logger.warning(f"⚠️  {slug}: Error parsing DEX volume data: {e}")
            response.meta['dex_volume_data'] = {}
        
        # Continue to treasury chart
        return self.fetch_treasury_chart(response.meta)
    
    def fetch_treasury_chart(self, meta):
        """Fetch treasury chart"""
        slug = meta['slug']
        treasury_url = f'https://api.llama.fi/treasury/{slug}'
        
        return scrapy.Request(
            url=treasury_url,
            callback=self.parse_treasury_chart,
            meta=meta,
            dont_filter=True,
            errback=lambda f: self.merge_and_yield(f.request.meta)
        )
    
    def parse_treasury_chart(self, response):
        """Parse treasury chart data"""
        slug = response.meta['slug']
        
        try:
            if response.status == 200:
                data = json.loads(response.text)
                
                # Treasury data is stored in chainTvls
                # Treasury = OwnTokens chain + regular chains (excluding -OwnTokens chains)
                # Chains ending with "-OwnTokens" are subsets already included in "OwnTokens"
                chain_tvls = data.get('chainTvls', {})
                
                # Dictionary to store treasury values by timestamp
                treasury_by_timestamp = {}
                
                # First pass: Collect values by timestamp and chain type
                own_tokens_by_timestamp = {}
                regular_chains_by_timestamp = {}
                
                for chain_name, chain_data in chain_tvls.items():
                    tvl_list = chain_data.get('tvl', [])
                    
                    # Skip chains ending with "-OwnTokens" (they're subsets)
                    if chain_name.endswith('-OwnTokens'):
                        continue
                    
                    for entry in tvl_list:
                        timestamp = entry.get('date')
                        if timestamp:
                            timestamp_str = str(timestamp)
                            total_liquidity = entry.get('totalLiquidityUSD', 0)
                            
                            if total_liquidity > 0:
                                # Separate OwnTokens chain from regular chains
                                if chain_name == 'OwnTokens':
                                    own_tokens_by_timestamp[timestamp_str] = total_liquidity
                                else:
                                    # Regular chain (Ethereum, Arbitrum, etc.)
                                    if timestamp_str not in regular_chains_by_timestamp:
                                        regular_chains_by_timestamp[timestamp_str] = 0
                                    regular_chains_by_timestamp[timestamp_str] += total_liquidity
                
                # Combine OwnTokens + regular chains for each timestamp
                all_timestamps = set(own_tokens_by_timestamp.keys()) | set(regular_chains_by_timestamp.keys())
                for timestamp_str in all_timestamps:
                    own_tokens_value = own_tokens_by_timestamp.get(timestamp_str, 0)
                    regular_chains_value = regular_chains_by_timestamp.get(timestamp_str, 0)
                    treasury_by_timestamp[timestamp_str] = own_tokens_value + regular_chains_value
                
                # Convert to date-indexed dictionary for easier merging
                treasury_by_date = {}
                for timestamp_str, treasury_value in treasury_by_timestamp.items():
                    try:
                        date_str = datetime.fromtimestamp(int(timestamp_str)).strftime('%Y-%m-%d')
                        treasury_by_date[date_str] = {
                            'treasury': treasury_value,
                            'timestamp': timestamp_str
                        }
                    except:
                        pass
                
                response.meta['treasury_data'] = treasury_by_date
                self.logger.debug(f"✅ {slug}: Treasury data - {len(treasury_by_date)} records")
                
                # Extract governanceID from treasury API response
                governance_id = None
                governance_type = None
                governance_id_array = data.get('governanceID', [])
                
                if governance_id_array and len(governance_id_array) > 0:
                    if len(governance_id_array) == 1:
                        # Single governance ID: use it
                        gov_id_str = governance_id_array[0]
                        if ':' in gov_id_str:
                            governance_type, governance_id = gov_id_str.split(':', 1)
                        else:
                            governance_id = gov_id_str
                            governance_type = 'snapshot'  # Default assumption
                    else:
                        # Multiple governance IDs (> 1): filter out snapshot (off-chain), use the other one
                        # Examples: ["snapshot:aave.eth", "tally:eip155/1/0x..."] -> use tally
                        for gov_id_str in governance_id_array:
                            if ':' in gov_id_str:
                                gov_type, gov_id = gov_id_str.split(':', 1)
                                if gov_type.lower() != 'snapshot':
                                    # Use the non-snapshot one (could be tally, compound, etc.)
                                    governance_type = gov_type
                                    governance_id = gov_id
                                    break
                        # If we didn't find a non-snapshot one, fall back to first
                        if not governance_id:
                            gov_id_str = governance_id_array[0]
                            if ':' in gov_id_str:
                                governance_type, governance_id = gov_id_str.split(':', 1)
                            else:
                                governance_id = gov_id_str
                                governance_type = 'snapshot'
                
                # If governanceID exists, fetch governance data
                if governance_id and governance_type:
                    return self.fetch_governance_data(response.meta, governance_type, governance_id)
        
        except Exception as e:
            self.logger.warning(f"⚠️  {slug}: Error parsing treasury data: {e}")
            response.meta['treasury_data'] = {}
        
        # Continue to fetch emission/unlock data
        return self.fetch_emission_data(response.meta)
    
    def fetch_governance_data(self, meta, governance_type, governance_id):
        """Fetch governance data from DeFiLlama governance cache"""
        slug = meta['slug']
        
        # Build governance API URL based on governance type
        # Format: https://defillama-datasets.llama.fi/governance-cache/{type}/{governance_id}.json
        # Examples:
        # - snapshot: https://defillama-datasets.llama.fi/governance-cache/snapshot/aave.eth.json
        # - tally: https://defillama-datasets.llama.fi/governance-cache/tally/eip155/1/0xec568fffba86c094cf06b22134b23074dfe2252c.json
        # - compound: https://defillama-datasets.llama.fi/governance-cache/compound/ethereum/0xabc.json
        # Use the governance_type we extracted (could be snapshot, tally, compound, etc.)
        
        governance_url = f'https://defillama-datasets.llama.fi/governance-cache/{governance_type}/{governance_id}.json'
        
        return scrapy.Request(
            url=governance_url,
            callback=self.parse_governance_data,
            meta=meta,
            dont_filter=True,
            errback=lambda f: self.merge_and_yield(f.request.meta)
        )
    
    def parse_governance_data(self, response):
        """Parse governance data and extract monthly proposals metrics"""
        slug = response.meta['slug']
        
        try:
            if response.status == 200:
                data = json.loads(response.text)
                
                # Governance data structure:
                # - Monthly summaries: stats.months["2023-12"]: { "proposals": [...], "total": 26, "successful": 9 }
                # - Proposal details: "proposals": { "proposal_id": { "scores_total": ..., "month": "2023-12" } }
                
                proposals_obj = data.get('proposals', {})
                stats = data.get('stats', {})
                months_data = stats.get('months', {})
                
                # Dictionary to store governance metrics by month (YYYY-MM format)
                governance_by_month = {}
                
                # Process monthly summaries from stats.months
                for month_key, month_data in months_data.items():
                    # Check if it's a month key (format: YYYY-MM)
                    if re.match(r'^\d{4}-\d{2}$', month_key):
                        proposal_ids = month_data.get('proposals', [])
                        total_proposals = month_data.get('total', 0)
                        successful_proposals = month_data.get('successful', 0)
                        
                        # Find maxvotes (max scores_total) for this month
                        max_votes = 0
                        for proposal_id in proposal_ids:
                            if proposal_id in proposals_obj:
                                proposal = proposals_obj[proposal_id]
                                scores_total = proposal.get('scores_total', 0)
                                if scores_total > max_votes:
                                    max_votes = scores_total
                        
                        governance_by_month[month_key] = {
                            'totalproposals': total_proposals,
                            'successfulproposals': successful_proposals,
                            'maxvotes': max_votes if max_votes > 0 else None
                        }
                
                # Convert to date-indexed dictionary for easier merging
                # Only store governance data for the first day of each month (e.g., 2025-02-01)
                governance_by_date = {}
                for month_key, gov_data in governance_by_month.items():
                    # Store only for the first day of the month
                    year, month = month_key.split('-')
                    date_str = f"{year}-{month}-01"
                    governance_by_date[date_str] = gov_data.copy()
                
                response.meta['governance_data'] = governance_by_date
                self.logger.debug(f"✅ {slug}: Governance data - {len(governance_by_month)} months, {len(governance_by_date)} dates")
        
        except Exception as e:
            self.logger.warning(f"⚠️  {slug}: Error parsing governance data: {e}")
            response.meta['governance_data'] = {}
        
        # Continue to fetch emission/unlock data
        return self.fetch_emission_data(response.meta)
    
    def fetch_emission_data(self, meta):
        """Fetch emission data to extract unlock information"""
        slug = meta['slug']
        emission_url = f'https://api.llama.fi/emission/{slug}'
        
        return scrapy.Request(
            url=emission_url,
            callback=self.parse_emission_data,
            meta=meta,
            dont_filter=True,
            headers={
                'accept': '*/*',
                'accept-encoding': 'gzip, deflate, br, zstd',
                'accept-language': 'en-US,en;q=0.9',
                'origin': 'https://defillama.com',
                'referer': 'https://defillama.com/',
                'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36 Edg/144.0.0.0'
            },
            errback=lambda f: self.merge_and_yield(f.request.meta)
        )
    
    def parse_emission_data(self, response):
        """Parse emission data and extract unlockUsdChart"""
        slug = response.meta['slug']
        
        try:
            if response.status == 200:
                # Handle gzip decompression if needed
                response_body = response.body
                if response_body[:2] == b'\x1f\x8b':
                    response_body = gzip.decompress(response_body)
                
                # Parse the response
                data = json.loads(response_body.decode('utf-8'))
                
                # Extract the body field which contains a JSON string
                body_str = data.get("body", "")
                
                if not body_str:
                    self.logger.debug(f"⚠️  {slug}: No 'body' field in emission data")
                    response.meta['unlocks_data'] = {}
                    response.meta['incentives_data'] = {}
                    return self.merge_and_yield(response.meta)
                
                # Parse the JSON string in the body field
                parsed_body = json.loads(body_str)
                
                # Extract unlockUsdChart for incentives
                unlock_usd_chart = parsed_body.get('unlockUsdChart', [])
                
                # Convert unlockUsdChart to date-indexed dictionary for incentives
                incentives_by_date = {}
                for entry in unlock_usd_chart:
                    if isinstance(entry, list) and len(entry) >= 2:
                        timestamp = entry[0]
                        incentive_value = entry[1]
                        
                        # Convert timestamp to date string
                        try:
                            date_str = datetime.fromtimestamp(int(timestamp)).strftime('%Y-%m-%d')
                            incentives_by_date[date_str] = {
                                'incentives': incentive_value,
                                'timestamp': str(timestamp)
                            }
                        except Exception as e:
                            self.logger.debug(f"⚠️  {slug}: Error converting timestamp {timestamp} for incentives: {e}")
                            continue
                
                # Aggregate unlock data from documentedData.data.data (same logic as fetch_emission_data.py)
                # Dictionary to aggregate data by timestamp
                aggregated = defaultdict(lambda: {
                    'unlocked': 0,
                    'burned': 0,
                    'rawEmission': 0
                })
                
                # Navigate to documentedData.data
                documented_data = parsed_body.get('documentedData', {})
                data_array = documented_data.get('data', [])
                
                self.logger.debug(f"📊 {slug}: Processing {len(data_array)} emission categories...")
                
                # Iterate through each category (e.g., "LEND core development")
                for category in data_array:
                    label = category.get('label', 'Unknown')
                    category_data = category.get('data', [])
                    
                    # Iterate through each entry in the category
                    for entry in category_data:
                        timestamp = entry.get('timestamp')
                        unlocked = entry.get('unlocked', 0)
                        burned = entry.get('burned', 0)
                        raw_emission = entry.get('rawEmission', 0)
                        
                        if timestamp is not None:
                            # Sum values for the same timestamp
                            aggregated[timestamp]['unlocked'] += unlocked
                            aggregated[timestamp]['burned'] += burned
                            aggregated[timestamp]['rawEmission'] += raw_emission
                
                # Convert aggregated data to date-indexed dictionary
                unlocks_by_date = {}
                for timestamp in sorted(aggregated.keys()):
                    try:
                        date_str = datetime.fromtimestamp(int(timestamp)).strftime('%Y-%m-%d')
                        unlocks_by_date[date_str] = {
                            'unlocks': aggregated[timestamp]['unlocked'],
                            'burned': aggregated[timestamp]['burned'],
                            'rawEmission': aggregated[timestamp]['rawEmission'],
                            'timestamp': str(timestamp)
                        }
                    except Exception as e:
                        self.logger.debug(f"⚠️  {slug}: Error converting timestamp {timestamp}: {e}")
                        continue
                
                response.meta['unlocks_data'] = unlocks_by_date
                response.meta['incentives_data'] = incentives_by_date
                self.logger.debug(f"✅ {slug}: Unlocks data - {len(unlocks_by_date)} records (aggregated from {len(data_array)} categories), Incentives data - {len(incentives_by_date)} records")
        
        except json.JSONDecodeError as e:
            self.logger.warning(f"⚠️  {slug}: Error parsing emission data JSON: {e}")
            response.meta['unlocks_data'] = {}
            response.meta['incentives_data'] = {}
        except Exception as e:
            self.logger.warning(f"⚠️  {slug}: Error parsing emission data: {e}")
            response.meta['unlocks_data'] = {}
            response.meta['incentives_data'] = {}
        
        # Finally merge all data and yield
        return self.merge_and_yield(response.meta)
    
    def merge_and_yield(self, meta):
        """Merge all collected data and yield items"""
        protocol_name = meta['protocol_name']
        slug = meta['slug']
        
        # Get all data sources
        tvl_data = meta.get('tvl_data', {})
        price_data = meta.get('price_data', {})
        mcap_data = meta.get('mcap_data', {})
        volume_data = meta.get('volume_data', {})
        max_supply = meta.get('max_supply')
        fees_data = meta.get('fees_data', {})
        revenue_data = meta.get('revenue_data', {})
        holders_revenue_data = meta.get('holders_revenue_data', {})
        dex_volume_data = meta.get('dex_volume_data', {})
        treasury_data = meta.get('treasury_data', {})
        governance_data = meta.get('governance_data', {})
        unlocks_data = meta.get('unlocks_data', {})
        incentives_data = meta.get('incentives_data', {})
        liquidity_data = meta.get('liquidity_data', {})

        
        # Collect all unique dates from all sources
        all_dates = set()
        
        # Add dates from TVL (convert timestamp to date)
        for timestamp in tvl_data.keys():
            try:
                date_str = datetime.fromtimestamp(int(timestamp)).strftime('%Y-%m-%d')
                all_dates.add(date_str)
            except:
                pass
        
        # Add dates from other sources
        all_dates.update(price_data.keys())
        all_dates.update(mcap_data.keys())
        all_dates.update(volume_data.keys())
        all_dates.update(fees_data.keys())
        all_dates.update(revenue_data.keys())
        all_dates.update(holders_revenue_data.keys())
        all_dates.update(dex_volume_data.keys())
        all_dates.update(treasury_data.keys())
        all_dates.update(governance_data.keys())
        all_dates.update(unlocks_data.keys())
        all_dates.update(incentives_data.keys())
        all_dates.update(liquidity_data.keys())
        
        # Create merged rows
        rows_count = 0
        for date_str in sorted(all_dates):
            # Find timestamp for this date (prefer from fees/revenue, then TVL)
            timestamp = None
            
            # Try to get timestamp from various sources
            if date_str in fees_data:
                timestamp = fees_data[date_str].get('timestamp')
            elif date_str in revenue_data:
                timestamp = revenue_data[date_str].get('timestamp')
            elif date_str in dex_volume_data:
                timestamp = dex_volume_data[date_str].get('timestamp')
            elif date_str in treasury_data:
                timestamp = treasury_data[date_str].get('timestamp')
            elif date_str in unlocks_data:
                timestamp = unlocks_data[date_str].get('timestamp')
            elif date_str in liquidity_data:
                timestamp = liquidity_data[date_str].get('timestamp')
            elif date_str in price_data:
                timestamp = price_data[date_str].get('timestamp')
            else:
                # Convert date back to timestamp
                try:
                    dt = datetime.strptime(date_str, '%Y-%m-%d')
                    timestamp = str(int(dt.timestamp()))
                except:
                    timestamp = ''
            
            # Get TVL for this timestamp/date
            tvl_value = tvl_data.get(timestamp) if timestamp else None
            
            # Get unlock USD and token price for calculating unlock tokens
            unlock_usd = unlocks_data.get(date_str, {}).get('unlocks')
            token_price = price_data.get(date_str, {}).get('tokenprice')
            
            # Calculate unlock tokens: unlock_usd / token_price
            unlock_tokens = None
            if unlock_usd is not None and token_price is not None and token_price > 0:
                try:
                    unlock_tokens = unlock_usd / token_price
                except (ZeroDivisionError, TypeError):
                    unlock_tokens = None
            
            # Calculate FDV: max_supply * token_price (simple calculation)
            fdv_value = None
            if max_supply is not None and max_supply > 0 and token_price is not None:
                try:
                    fdv_value = max_supply * token_price
                except (TypeError, ValueError):
                    fdv_value = None
            
            # Build row
            row = {
                'protocol_name': protocol_name,
                'timestamp': timestamp,
                'date': date_str,
                'tvl': tvl_value,
                'mcap': mcap_data.get(date_str, {}).get('mcap'),
                'tokenprice': token_price,
                'tokenvolume': volume_data.get(date_str, {}).get('tokenvolume'),
                'fdv': fdv_value,
                'tokenliquidity': liquidity_data.get(date_str, {}).get('tokenliquidity'),
                'fees': fees_data.get(date_str, {}).get('fees'),
                'revenue': revenue_data.get(date_str, {}).get('revenue'),
                'holdersrevenue': holders_revenue_data.get(date_str, {}).get('holdersrevenue'),
                'dexvolume': dex_volume_data.get(date_str, {}).get('dexvolume'),
                'unlocks': unlock_usd,
                'unlockstokens': unlock_tokens,
                'incentives': incentives_data.get(date_str, {}).get('incentives'),
                'totalproposals': governance_data.get(date_str, {}).get('totalproposals'),
                'successfulproposals': governance_data.get(date_str, {}).get('successfulproposals'),
                'maxvotes': governance_data.get(date_str, {}).get('maxvotes'),
                'treasury': treasury_data.get(date_str, {}).get('treasury')
            }
            
            yield row
            rows_count += 1
        
        self.logger.info(f"✅ {slug}: {rows_count} merged rows")
        return []
    


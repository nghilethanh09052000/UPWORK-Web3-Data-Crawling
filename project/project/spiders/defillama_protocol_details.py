import scrapy
import json
import os
import re
from urllib.parse import urlparse


class DefillamaProtocolDetailsSpider(scrapy.Spider):
    name = "defillama_protocol_details"
    
    custom_settings = {
        'USER_AGENT': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36 Edg/142.0.0.0',
        'CONCURRENT_REQUESTS': 10,
        #'DOWNLOAD_DELAY': 0.2,
    }
    
    def __init__(self):
        # Load protocols_list.json to get chains data
        protocols_file = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'protocols_list_missing.json')
        try:
            with open(protocols_file, 'r', encoding='utf-8') as f:
                self.protocols_data = json.load(f)
            # Create a mapping of slug to chains
            self.slug_to_chains = {}
            for protocol in self.protocols_data:
                slug = protocol.get('slug')
                protocol_data = protocol.get('protocol_data', {})
                chains = protocol_data.get('chains', [])
                if slug and chains:
                    self.slug_to_chains[slug] = chains
        except Exception as e:
            self.logger.error(f"Error loading protocols_list.json: {e}")
            self.protocols_data = []
            self.slug_to_chains = {}
    
    def start_requests(self):
        # Read protocols from JSON file
        protocols_file = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'protocols_list_missing.json')
        
        try:
            with open(protocols_file, 'r', encoding='utf-8') as f:
                protocols = json.load(f)
            
            self.logger.info(f"Loaded {len(protocols)} protocols from {protocols_file}")
            
            # First, fetch protocol page to get buildId
            for protocol in protocols:
                slug = protocol.get('slug')
                name = protocol.get('name', slug)
                
                if slug:
                    # First request: get the protocol page to extract buildId
                    protocol_url = f'https://defillama.com/protocol/{slug}'
                    yield scrapy.Request(
                        url=protocol_url,
                        callback=self.parse_protocol_page,
                        headers={
                            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
                            'accept-language': 'en-US,en;q=0.9',
                            'referer': 'https://defillama.com/',
                            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36 Edg/142.0.0.0',
                            'cookie': '__stripe_mid=cd81240a-8592-42e9-b301-24ceeae0011545fd3a; cf_clearance=oCRcJrN9AoMPDM9fnEUzy83Jc8AIbGsezcpCYXVUk_0-1770135388-1.2.1.1-iX39dYtUPM2gN5.j.a6ko15eXqNo4M2Ypkvomk71GFQ9nH2tY.pKG7bL4JJOQ4r_zmLqQ9bK.OntVf7AsFUTdgQAKw2jZGKL_xOdCC0MJvaxEfAPvQsc6wpb8gP7zp5QC1ym9M1XGywyOuG25v_8TUfQUM7ePVauoxp9r4VfbpLQcNruqFEr06NqIIRO1dncaIAPq8OeeuMqNLbZhWptZFaAOKAYjZVlDVdBZiXWS3QvrtNUVO2g1kw0YlDdMLww'
                        },
                        meta={
                            'slug': slug,
                            'name': name,
                            'protocol': protocol
                        },
                        dont_filter=True
                    )
        except FileNotFoundError:
            self.logger.error(f"Protocols file not found: {protocols_file}")
        except json.JSONDecodeError as e:
            self.logger.error(f"Error parsing protocols JSON: {e}")
    
    def parse_protocol_page(self, response):
        """Extract pageProps from __NEXT_DATA__ and process"""
        slug = response.meta.get('slug')
        name = response.meta.get('name')
        protocol = response.meta.get('protocol')
        
        # Extract pageProps from __NEXT_DATA__
        script_data = response.xpath('//script[@id="__NEXT_DATA__"]/text()').get()
        
        if script_data:
            try:
                next_data = json.loads(script_data)
                page_props = next_data.get('props', {}).get('pageProps', {})
                
                if page_props:
                    self.logger.info(f"Extracted pageProps from __NEXT_DATA__ for {slug}")
                    # Process pageProps directly
                    for request in self.process_page_props(page_props, slug, name, protocol):
                        yield request
                else:
                    self.logger.warning(f"No pageProps found in __NEXT_DATA__ for {slug}")
            except json.JSONDecodeError as e:
                self.logger.error(f"JSON decode error for {slug}: {e}")
        else:
            self.logger.warning(f"Could not find __NEXT_DATA__ script tag for {slug}")
    
    def process_page_props(self, page_props, slug, name, protocol):
        """Process pageProps and create requests for treasury/governance"""
        # Extract basic fields
        protocol_id = page_props.get('id', '')
        protocol_name = page_props.get('name', name)
        website = page_props.get('website', '')
        description = page_props.get('description', '')
        logo = f'https://icons.llamao.fi/icons/protocols/{slug}'
        
        # Extract token data
        token = page_props.get('token', {})
        gecko_id = token.get('gecko_id', '')
        symbol = token.get('symbol', '')
        explorer_url = token.get('explorer_url', '')
        
        # Extract address from explorer_url
        address = ''
        if explorer_url:
            # Extract address from URL like: https://etherscan.io/token/0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9
            address_match = re.search(r'/(0x[a-fA-F0-9]{40})', explorer_url)
            if address_match:
                address = address_match.group(1)
        
        # Extract social links
        twitter = page_props.get('twitter', '')
        github_list = page_props.get('github', [])
        github = github_list[0] if github_list else ''
        
        # Get chains from protocols_list.json
        chains = self.slug_to_chains.get(slug, [])
        
        # Prepare meta for treasury request
        meta_data = {
            'slug': slug,
            'name': protocol_name,
            'id': protocol_id,
            'url': website,
            'description': description,
            'logo': logo,
            'gecko_id': gecko_id,
            'cmcId': None,
            'chains': chains,
            'twitter': twitter,
            'github': github,
            'symbol': symbol,
            'address': address,
            'treasury': None,
            'governance_id': None
        }
        
        # Request treasury data
        treasury_url = f'https://api.llama.fi/treasury/{slug}'
        yield scrapy.Request(
            url=treasury_url,
            callback=self.parse_treasury,
            headers={
                'accept': '*/*',
                'referer': 'https://defillama.com/',
                'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36 Edg/142.0.0.0',
                            'cookie': '__stripe_mid=cd81240a-8592-42e9-b301-24ceeae0011545fd3a; cf_clearance=mzqJOjrwkCtOgg_r5lV7UHKFyJBrespz6xuBzGGrqVg-1770132920-1.2.1.1-Va0sDCtuCTazwfIC1WGnB7uaBG2o33iQxXZjoQi8VrLv34ApPh_bQZohRDTLQJ55agGVKN5iJZqiMlPZOMgIlrSeENKyHV0fWgD_R9Sm4_C1Bvb5eJXFjj_WYVxrtLqYGaMzMVPU2aphc7DcCp7ut13JBSumD9vFMCRYtrcTMdg3E36YzSCG8usTAGjcBvKCqocjK78gZBHAL5.Mnl5eRZubFPWy358178ulLb3llJ4JUM7Gh0ksZIft7hJ36Qgg'
            },
            meta=meta_data,
            dont_filter=True,
            errback=self.handle_treasury_error
        )
    
    def parse_treasury(self, response):
        """Parse treasury API response and extract treasury, cmcId, and governanceID"""
        meta_data = response.meta.copy()
        slug = meta_data.get('slug')
        
        # Parse treasury data
        treasury_data = None
        try:
            treasury_data = json.loads(response.text)
            
            # Extract treasury, cmcId, and governanceID[0] from treasury response
            if treasury_data:
                # Get treasury field (e.g., "aave.js")
                treasury_value = treasury_data.get('treasury')
                meta_data['treasury'] = treasury_value
                
                # Get cmcId field
                cmcId = treasury_data.get('cmcId')
                meta_data['cmcId'] = cmcId
                
                # Get governanceID[0] (first element of governanceID array)
                governanceID = treasury_data.get('governanceID', [])
                if governanceID and len(governanceID) > 0:
                    meta_data['governance_id'] = governanceID[0]
                else:
                    meta_data['governance_id'] = None
            else:
                meta_data['treasury'] = None
                meta_data['governance_id'] = None
                
        except json.JSONDecodeError:
            self.logger.warning(f"Could not parse treasury data for {slug}")
            meta_data['treasury'] = None
            meta_data['governance_id'] = None
        
        # Yield final result
        yield self.create_final_item(meta_data)
    
    def create_final_item(self, meta_data):
        """Create the final item with all extracted data"""
        return {
            'id': meta_data.get('id', ''),
            'name': meta_data.get('name', ''),
            'url': meta_data.get('url', ''),
            'description': meta_data.get('description', ''),
            'logo': meta_data.get('logo', ''),
            'gecko_id': meta_data.get('gecko_id', ''),
            'cmcId': meta_data.get('cmcId'),
            'chains': meta_data.get('chains', []),  # Keep as array, not JSON string
            'twitter': meta_data.get('twitter', ''),
            'treasury': meta_data.get('treasury'),
            'governance_id': meta_data.get('governance_id'),
            'github': meta_data.get('github', ''),
            'symbol': meta_data.get('symbol', ''),
            'address': meta_data.get('address', '')
        }
    
    def handle_treasury_error(self, failure):
        """Handle treasury API errors"""
        meta_data = failure.request.meta.copy()
        slug = meta_data.get('slug')
        self.logger.warning(f"Treasury API error for {slug}: {failure.value}")
        
        # Continue without treasury data
        meta_data['treasury'] = None
        meta_data['cmcId'] = None
        meta_data['governance_id'] = None
        
        # Yield final result
        return self.create_final_item(meta_data)
    
    def handle_error(self, failure):
        """Handle general errors"""
        slug = failure.request.meta.get('slug', 'Unknown')
        self.logger.error(f"Error fetching data for {slug}: {failure.value}")


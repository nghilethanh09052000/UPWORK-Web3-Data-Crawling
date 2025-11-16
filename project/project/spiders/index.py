import scrapy
import json
import re


class IndexSpider(scrapy.Spider):
    name = "index"
    
    custom_settings = {
        'USER_AGENT': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36 Edg/142.0.0.0',
    }
    
    def start_requests(self):
        url = 'https://defillama.com/protocols'
        yield scrapy.Request(
            url=url, 
            callback=self.parse,
            headers={
                'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                'accept-encoding': 'gzip, deflate, br, zstd',
                'accept-language': 'en-US,en;q=0.9.9',
                'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36 Edg/142.0.0.0'
            }
        )

    def parse(self, response):
        # Extract JSON from __NEXT_DATA__ script tag
        script_data = response.xpath('//script[@id="__NEXT_DATA__"]/text()').get()
        
        if script_data:
            # Parse the JSON
            data = json.loads(script_data)
            
            # Get protocols list
            protocols = data.get('props', {}).get('pageProps', {}).get('protocols', [])
            
            self.logger.info(f"Found {len(protocols)} protocols")
            
            # Make emission API requests for each protocol
            for protocol in protocols:
                slug = protocol.get('slug')
                name = protocol.get('name')
                
                if slug:
                    # Request emission data for this protocol
                    emission_url = f'https://api.llama.fi/emission/{slug}'
                    yield scrapy.Request(
                        url=emission_url,
                        callback=self.parse_emission,
                        headers={
                            'accept': '*/*',
                            'accept-encoding': 'gzip, deflate, br, zstd',
                            'accept-language': 'en-US,en;q=0.9',
                            'origin': 'https://defillama.com',
                            'priority': 'u=1, i',
                            'referer': 'https://defillama.com/',
                            'sec-ch-ua': '"Chromium";v="142", "Microsoft Edge";v="142", "Not_A Brand";v="99"',
                            'sec-ch-ua-mobile': '?0',
                            'sec-ch-ua-platform': '"macOS"',
                            'sec-fetch-dest': 'empty',
                            'sec-fetch-mode': 'cors',
                            'sec-fetch-site': 'cross-site',
                            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36 Edg/142.0.0.0'
                        },
                        meta={
                            'protocol_name': name,
                            'protocol_slug': slug,
                            'protocol_data': protocol
                        },
                        dont_filter=True,
                        errback=self.handle_error
                    )
                    self.logger.info(f"Requesting emission data for: {name} (slug: {slug})")
        else:
            self.logger.error("Could not find __NEXT_DATA__ script tag")
    
    def parse_emission(self, response):
        """Parse emission API response and extract stakeholder information"""
        protocol_name = response.meta.get('protocol_name')
        protocol_slug = response.meta.get('protocol_slug')
        
        try:
            # First parse: get the outer JSON
            emission_data = json.loads(response.text)
            
            # Second parse: the body field contains escaped JSON string
            body_str = emission_data.get('body', '{}')
            body_data = json.loads(body_str)
            
            # Extract stakeholder information from documentedData
            documented_data = body_data.get('documentedData', {}).get('data', [])
            
            # Extract stakeholder names and their token holdings (last unlocked value)
            for stakeholder in documented_data:
                stakeholder_name = stakeholder.get('label', '')
                data_points = stakeholder.get('data', [])
                
                # Get the last element's unlocked value (current tokens held)
                if data_points:
                    last_data_point = data_points[-1]
                    tokens_held = last_data_point.get('unlocked', 0)
                    
                    yield {
                        'protocol_name': protocol_name,
                        'stakeholder_name': stakeholder_name,
                        'tokens_held': tokens_held
                    }
            
            self.logger.info(f"Extracted {len(documented_data)} stakeholders for {protocol_name}")
            
        except json.JSONDecodeError as e:
            self.logger.warning(f"Could not parse emission data for {protocol_name} (slug: {protocol_slug}): {str(e)}")
        except Exception as e:
            self.logger.error(f"Error processing emission data for {protocol_name}: {str(e)}")
    
    def handle_error(self, failure):
        """Handle errors in emission API requests"""
        protocol_name = failure.request.meta.get('protocol_name', 'Unknown')
        self.logger.warning(f"Failed to fetch emission data for {protocol_name}: {str(failure.value)}")

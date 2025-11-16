import scrapy
import json
import os


class Index2Spider(scrapy.Spider):
    name = "index_2"
    
    custom_settings = {
        'USER_AGENT': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36 Edg/142.0.0.0',
    }
    
    def start_requests(self):
        # Read protocols from JSON file (default: protocols_list.json in project directory)
        protocols_file = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'protocols_list.json')
        
        try:
            with open(protocols_file, 'r', encoding='utf-8') as f:
                protocols = json.load(f)
            
            self.logger.info(f"Loaded {len(protocols)} protocols from {protocols_file}")
            
            # Visit each protocol detail page
            for protocol in protocols:
                slug = protocol.get('slug')
                
                if slug:
                    protocol_url = f'https://defillama.com/protocol/{slug}'
                    yield scrapy.Request(
                        url=protocol_url,
                        callback=self.parse_protocol,
                        headers={
                            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                            'accept-encoding': 'gzip, deflate, br, zstd',
                            'accept-language': 'en-US,en;q=0.9',
                            'sec-ch-ua': '"Chromium";v="142", "Microsoft Edge";v="142", "Not_A Brand";v="99"',
                            'sec-ch-ua-mobile': '?0',
                            'sec-ch-ua-platform': '"macOS"',
                            'sec-fetch-dest': 'document',
                            'sec-fetch-mode': 'navigate',
                            'sec-fetch-site': 'none',
                            'sec-fetch-user': '?1',
                            'upgrade-insecure-requests': '1',
                            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36 Edg/142.0.0.0'
                        }
                    )
        except FileNotFoundError:
            self.logger.error(f"Protocols file not found: {protocols_file}")
            self.logger.error("Please run: scrapy crawl extract_protocols -o protocols_list.json")
        except json.JSONDecodeError as e:
            self.logger.error(f"Error parsing protocols JSON: {e}")
    
    def parse_protocol(self, response):
        """Parse protocol detail page and extract slug, github, website, twitter"""
        # Get slug from URL
        slug = response.url.split('/')[-1]
        defillama_url = response.url
        
        # Find the Protocol Information section
        protocol_info_section = response.xpath('//h2[@id="protocol-information"]/following-sibling::div[@class="flex flex-wrap gap-2"]')
        
        if protocol_info_section:
            # Extract ALL links from the Protocol Information section
            all_links = protocol_info_section.xpath('.//a/@href').getall()
            
            # Filter links by type (exclude DefiLlama and safeharbor links)
            github_links = []
            website_links = []
            twitter_links = []
            
            for link in all_links:
                if 'github.com' in link and 'DefiLlama' not in link:
                    github_links.append(link)
                elif 'twitter.com' in link or 'x.com' in link:
                    twitter_links.append(link)
                elif link.startswith('http') and 'safeharbor' not in link.lower():
                    website_links.append(link)
            
            # Remove duplicates while preserving order
            github_links = list(dict.fromkeys(github_links))
            website_links = list(dict.fromkeys(website_links))
            twitter_links = list(dict.fromkeys(twitter_links))
            
            # Get the maximum count to determine number of rows
            max_count = max(len(github_links), len(website_links), len(twitter_links), 1)
            
            # Create rows for each combination
            for i in range(max_count):
                yield {
                    'name': slug,
                    'defillama_url': defillama_url,
                    'github': github_links[i] if i < len(github_links) else '',
                    'website': website_links[i] if i < len(website_links) else '',
                    'twitter': twitter_links[i] if i < len(twitter_links) else ''
                }
            
            self.logger.info(f"Extracted data for: {slug} (GitHub: {len(github_links)}, Website: {len(website_links)}, Twitter: {len(twitter_links)})")
        else:
            self.logger.warning(f"Could not find Protocol Information section for: {slug}")

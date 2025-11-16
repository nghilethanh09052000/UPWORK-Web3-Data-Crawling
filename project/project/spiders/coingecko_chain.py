import scrapy
import re


class CoingeckoChainSpider(scrapy.Spider):
    name = "coingecko_chain"
    
    custom_settings = {
        'USER_AGENT': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36 Edg/142.0.0.0',
        'LOG_FILE': f"coingecko.log",
    }
    
    def start_requests(self):
        # Loop through pages 1 to 3
        for page in range(1, 4):
            url = f'https://www.coingecko.com/en/chains?page={page}'
            
            yield scrapy.Request(
                url=url, 
                callback=self.parse,
                headers={
                    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                    'accept-encoding': 'gzip, deflate, br, zstd',
                    'accept-language': 'en-US,en;q=0.9',
                    'priority': 'u=0, i',
                    'sec-ch-ua': '"Chromium";v="142", "Microsoft Edge";v="142", "Not_A Brand";v="99"',
                    'sec-ch-ua-mobile': '?0',
                    'sec-ch-ua-platform': '"macOS"',
                    'sec-fetch-dest': 'document',
                    'sec-fetch-mode': 'navigate',
                    'sec-fetch-site': 'none',
                    'sec-fetch-user': '?1',
                    'upgrade-insecure-requests': '1',
                    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36 Edg/142.0.0.0'
                },
                meta={'page': page},
                dont_filter=True
            )
    
    def parse(self, response):
        """Extract all chain links from the chains page"""
        page_num = response.meta.get('page', 'unknown')
        
        # Use regex to find all /en/chains/* links
        chain_pattern = r'/en/chains/([a-zA-Z0-9\-]+)'
        chain_slugs = re.findall(chain_pattern, response.text)
        
        # Remove duplicates
        chain_slugs = list(set(chain_slugs))
        
        self.logger.info(f"PAGE {page_num}: Found {len(chain_slugs)} unique chain slugs in {response.url}")
        self.logger.info(f"PAGE {page_num}: Chain slugs: {chain_slugs}")
        
        # Visit each chain page first
        for chain_slug in chain_slugs:
            chain_url = f'https://www.coingecko.com/en/chains/{chain_slug}'
            yield { 
                'name': chain_slug,
                'chain_url': chain_url
            }
        
    def parse_chain(self, response):
        """Parse chain page and extract coin links to check if chain has a coin page"""
        chain_slug = response.meta.get('chain_slug')
        page_num = response.meta.get('page', 'unknown')
        
        # Extract the table with data-page="chainsShow"
        table = response.xpath('//table[@data-page="chainsShow"]').get()
        
        if not table:
            self.logger.warning(f"PAGE {page_num}: Could not find table with data-page='chainsShow' for chain: {chain_slug}")
            return
        
        # Use regex to find all /en/coins/* links within the table
        coin_pattern = r'/en/coins/([a-zA-Z0-9\-]+)'
        coin_slugs = re.findall(coin_pattern, table)
        
        # Remove duplicates
        coin_slugs = list(set(coin_slugs))
        
        self.logger.info(f"PAGE {page_num}: Found {len(coin_slugs)} coin links in chain: {chain_slug}")
        
        # Check if the chain_slug exists in the coin slugs
        if chain_slug in coin_slugs:
            self.logger.info(f"PAGE {page_num}: Chain {chain_slug} has a coin page, requesting /en/coins/{chain_slug}")
            
            # Make request to /en/coins/{chain_slug}
            coin_url = f'https://www.coingecko.com/en/coins/{chain_slug}'
            yield scrapy.Request(
                url=coin_url,
                callback=self.parse_coin,
                headers={
                    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                    'accept-encoding': 'gzip, deflate, br, zstd',
                    'accept-language': 'en-US,en;q=0.9',
                    'priority': 'u=0, i',
                    'sec-ch-ua': '"Chromium";v="142", "Microsoft Edge";v="142", "Not_A Brand";v="99"',
                    'sec-ch-ua-mobile': '?0',
                    'sec-ch-ua-platform': '"macOS"',
                    'sec-fetch-dest': 'document',
                    'sec-fetch-mode': 'navigate',
                    'sec-fetch-site': 'none',
                    'sec-fetch-user': '?1',
                    'upgrade-insecure-requests': '1',
                    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36 Edg/142.0.0.0'
                },
                meta={'chain_slug': chain_slug}
            )
        else:
            self.logger.info(f"PAGE {page_num}: Chain {chain_slug} does not have a matching coin page, skipping")
    
    def parse_coin(self, response):
        """Parse coin detail page and extract github, website, twitter links"""
        chain_slug = response.meta.get('chain_slug')
        coin_url = response.url
        
        # Extract Website links from the "Website" section
        website_section = response.xpath('//div[contains(text(), "Website")]/following-sibling::div[1]')
        website_links = website_section.xpath('.//a/@href').getall()
        
        # Extract Community links from the "Community" section
        community_section = response.xpath('//div[contains(text(), "Community")]/following-sibling::div[1]')
        community_links = community_section.xpath('.//a/@href').getall()
        
        # Filter community links for GitHub and Twitter/X
        github_links = []
        twitter_links = []
        
        for link in community_links:
            if 'github.com' in link:
                github_links.append(link)
            elif 'twitter.com' in link or 'x.com' in link:
                twitter_links.append(link)
        
        # If no GitHub or Twitter links found in Community section, use regex as fallback
        if not github_links or not twitter_links:
            # Use regex to find links in the entire page
            github_pattern = r'https?://(?:www\.)?github\.com/[a-zA-Z0-9\-_]+(?:/[a-zA-Z0-9\-_]+)?'
            twitter_pattern = r'https?://(?:www\.)?(?:twitter\.com|x\.com)/[a-zA-Z0-9_]+'
            
            if not github_links:
                github_matches = re.findall(github_pattern, response.text)
                github_links = [link for link in github_matches if 'coingecko' not in link.lower()]
            
            if not twitter_links:
                twitter_matches = re.findall(twitter_pattern, response.text)
                twitter_links = [link for link in twitter_matches if '/search?' not in link and 'coingecko' not in link.lower()]
        
        # Remove duplicates while preserving order
        github_links = list(dict.fromkeys(github_links))
        twitter_links = list(dict.fromkeys(twitter_links))
        website_links = list(dict.fromkeys(website_links))
        
        # Get maximum count to determine number of rows
        max_count = max(len(github_links), len(twitter_links), len(website_links), 1)
        
        # Create rows for each combination
        for i in range(max_count):
            yield {
                'name': chain_slug,
                'coin_url': coin_url,
                'github': github_links[i] if i < len(github_links) else '',
                'twitter': twitter_links[i] if i < len(twitter_links) else '',
                'website': website_links[i] if i < len(website_links) else ''
            }
        
        self.logger.info(f"Extracted data for coin: {chain_slug} (GitHub: {len(github_links)}, Twitter: {len(twitter_links)}, Website: {len(website_links)})")
import scrapy
import json


class ExtractProtocolsSpider(scrapy.Spider):
    name = "extract_protocols"
    
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
            }
        )

    def parse(self, response):
        # Extract JSON from __NEXT_DATA__ script tag
        script_data = response.xpath('//script[@id="__NEXT_DATA__"]/text()').get()
        
        if script_data:
            data = json.loads(script_data)
            protocols = data.get('props', {}).get('pageProps', {}).get('protocols', [])
            
            self.logger.info(f"Found {len(protocols)} protocols")
            
            # Yield each protocol
            for protocol in protocols:
                yield {
                    'name': protocol.get('name', ''),
                    'slug': protocol.get('slug', ''),
                    'protocol_data': protocol
                }
        else:
            self.logger.error("Could not find __NEXT_DATA__ script tag")


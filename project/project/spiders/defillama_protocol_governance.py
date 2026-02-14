import scrapy
import json
import os


class DefillamaGovernanceSpider(scrapy.Spider):
    name = "defillama_governance_simple"

    custom_settings = {
        #"CONCURRENT_REQUESTS": 8,
        "USER_AGENT": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
        "FEED_EXPORT_ENCODING": "utf-8",
        # Disable feed export since we're using a custom pipeline
        "FEEDS": {},
    }

    def start_requests(self):
        file_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
            "protocol_details.json"
        )

        with open(file_path, "r", encoding="utf-8") as f:
            protocols = json.load(f)

        total_protocols = len(protocols)
        skipped_with_gov = 0
        skipped_no_slug = 0
        queued = 0

        for protocol in protocols:
            # recrawl only missing governance_id
            if protocol.get("governance_id"):
                skipped_with_gov += 1
                continue

            slug = self.extract_slug_from_logo(protocol.get("logo"))
            if not slug:
                skipped_no_slug += 1
                continue

            queued += 1
            yield scrapy.Request(
                url=f"https://defillama.com/protocol/{slug}",
                callback=self.parse_protocol,
                meta={"slug": slug, "protocol_name": protocol.get("name", slug)},
                headers={
                        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
                        'accept-language': 'en-US,en;q=0.9',
                        'referer': 'https://defillama.com/',
                        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36 Edg/142.0.0.0',
                        'cookie': '__stripe_mid=cd81240a-8592-42e9-b301-24ceeae0011545fd3a; cf_clearance=fI7PDjgsxeIz0GtdHahN4Go4ZQTI6E9aYj61lbGtFPU-1770212304-1.2.1.1-PyxAOlScHo1NKto2luVbLvwgDQjx0__9s2XlB9e6L9_LGknUhsaDMzYwVnbbmljQvZPCTCgW14qRl0_CCkaH22CQmCY4qHxeSPB17ycF_8c_nEUkYOjwqLG5L.9aUojmyt4GKhea59sw_QqrLDM3UbJ2Z0F39uG7oL5O1NbgPInzii.OEpV23GtEe8207rLlezOFMruu_AXoY9bWYD7lkc4W.I8iWwqzDgQA3iWkNKI'
                    },
            )
        
        self.logger.info(f"Total protocols: {total_protocols}, Skipped (with gov): {skipped_with_gov}, Skipped (no slug): {skipped_no_slug}, Queued: {queued}")

    def extract_slug_from_logo(self, logo_url):
        if not logo_url:
            return None
        return logo_url.rstrip("/").split("/")[-1]

    def parse_protocol(self, response):
        slug = response.meta["slug"]
        protocol_name = response.meta.get("protocol_name", slug)

        if response.status != 200:
            self.logger.warning(f"Failed to fetch {slug}: HTTP {response.status}")
            return

        script = response.xpath('//script[@id="__NEXT_DATA__"]/text()').get()
        if not script:
            self.logger.debug(f"No __NEXT_DATA__ found for {slug}")
            return

        try:
            data = json.loads(script)
        except json.JSONDecodeError as e:
            self.logger.warning(f"Failed to parse __NEXT_DATA__ for {slug}: {e}")
            return

        page_props = data.get("props", {}).get("pageProps", {})
        governance_apis = page_props.get("governanceApis", [])

        if not governance_apis:
            self.logger.debug(f"No governanceApis found for {slug}")
            return

        governance_api_url = governance_apis[0]
        self.logger.info(f"Found governance API for {slug}: {governance_api_url}")

        yield scrapy.Request(
            url=governance_api_url,
            callback=self.parse_governance_api,
            meta={"slug": slug, "protocol_name": protocol_name},
            headers={
                    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
                    'accept-language': 'en-US,en;q=0.9',
                    'referer': 'https://defillama.com/',
                    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36 Edg/142.0.0.0',
                    'cookie': '__stripe_mid=cd81240a-8592-42e9-b301-24ceeae0011545fd3a; cf_clearance=fI7PDjgsxeIz0GtdHahN4Go4ZQTI6E9aYj61lbGtFPU-1770212304-1.2.1.1-PyxAOlScHo1NKto2luVbLvwgDQjx0__9s2XlB9e6L9_LGknUhsaDMzYwVnbbmljQvZPCTCgW14qRl0_CCkaH22CQmCY4qHxeSPB17ycF_8c_nEUkYOjwqLG5L.9aUojmyt4GKhea59sw_QqrLDM3UbJ2Z0F39uG7oL5O1NbgPInzii.OEpV23GtEe8207rLlezOFMruu_AXoY9bWYD7lkc4W.I8iWwqzDgQA3iWkNKI'
                },
        )

    def parse_governance_api(self, response):
        slug = response.meta["slug"]
        protocol_name = response.meta.get("protocol_name", slug)

        if response.status != 200:
            self.logger.warning(f"Failed to fetch governance API for {slug}: HTTP {response.status}")
            return

        try:
            data = json.loads(response.text)
        except json.JSONDecodeError as e:
            self.logger.warning(f"Failed to parse governance API JSON for {slug}: {e}")
            return

        metadata = data.get("metadata", {})
        governance_id = metadata.get("id")

        if not governance_id:
            self.logger.debug(f"No governance_id found in metadata for {slug}")
            return

        self.logger.info(f"Successfully extracted governance_id for {slug}: {governance_id}")
        yield {
            "slug": slug,
            "snapshot": f"snapshot:{governance_id}",
            "governance_id": governance_id,
            "url": response.url
        }

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import json
import os


class ProjectPipeline:
    def process_item(self, item, spider):
        return item


class JsonWriterPipeline:
    """Pipeline to write items to JSON file immediately"""
    
    def __init__(self):
        self.file = None
        self.items_written = 0
        
    def open_spider(self, spider):
        if spider.name == "defillama_governance_simple":
            # Get the project root directory (where scrapy.cfg is located)
            # pipelines.py is in project/pipelines.py
            # So we need to go up 2 levels: pipelines.py -> project -> root
            current_dir = os.path.dirname(os.path.abspath(__file__))
            project_dir = os.path.dirname(current_dir)
            file_path = os.path.join(project_dir, "protocol_governance.json")
            
            spider.logger.info(f"Opening file for writing: {file_path}")
            self.file = open(file_path, 'w', encoding='utf-8')
            self.file.write('[\n')
            self.items_written = 0
            
    def close_spider(self, spider):
        if self.file:
            self.file.write('\n]')
            self.file.close()
            spider.logger.info(f"Wrote {self.items_written} items to protocol_governance.json")
            
    def process_item(self, item, spider):
        if spider.name == "defillama_governance_simple":
            if not self.file:
                spider.logger.error("File not opened! Cannot write item.")
                return item
                
            try:
                if self.items_written > 0:
                    self.file.write(',\n')
                json.dump(dict(item), self.file, indent=2, ensure_ascii=False)
                self.file.flush()  # Force write to disk immediately
                self.items_written += 1
                spider.logger.debug(f"Wrote item {self.items_written}: {item.get('slug', 'unknown')}")
            except Exception as e:
                spider.logger.error(f"Error writing item: {e}")
        return item

import re
from datetime import datetime
from pathlib import Path

# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from scrapy.crawler import CrawlerProcess
from scrapy.exceptions import DropItem
from scrapy.exporters import CsvItemExporter
from scrapy.spiders import XMLFeedSpider

from items import JobItem


CURRENT_FILEPATH = Path(__file__).resolve().parent
DATA_FOLDER = CURRENT_FILEPATH.parent / 'data'
DATA_FOLDER.mkdir(exist_ok=True)
THIS_SPIDER_RESULT_FILE = DATA_FOLDER / 'chempostingcanada_jobs.csv'

JOB_TITLE_IGNORE_KEYWORDS = ['post-doc', 'postdoc', 'post doc', 'scientist']

FIELDS_TO_EXPORT = ['ads_title', 'posted_date', 'priority_date', 'category',
                    'school', 'department', 'specialization',
                    'rank', 'city', 'state', 'canada',
                    'current_status', 'comments1', 'comments2',
                    'ads_source', 'ads_job_code'
                    ]

COUNTRIES_TO_SEARCH = ['United States', 'Canada', 'Puerto Rico']


class CsvWriteLatestToOldest:
    """Write to CSV file in latest to oldest order of 'posted_date'"""
    def __init__(self, csv_export_file):
        self.csv_export_file = csv_export_file

    @classmethod
    def from_crawler(cls, crawler):
        """This is used to passed in parameter from setting
        Ref: https://docs.scrapy.org/en/latest/topics/item-pipeline.html?highlight=from_crawler#write-items-to-mongodb
        """

        return cls(
            csv_export_file=crawler.settings.get('CSV_EXPORT_FILE', THIS_SPIDER_RESULT_FILE),
        )

    def open_spider(self, spider):
        self.list_items = []
        self.file = open(self.csv_export_file, 'ab')

        # Creating a FanItemExporter object and initiating export
        self.exporter = CsvItemExporter(self.file, fields_to_export=FIELDS_TO_EXPORT)
        self.exporter.start_exporting()

    def close_spider(self, spider):
        ordered_list = sorted(self.list_items,
                              key=lambda i: i['posted_date'],
                              reverse=True)

        for i in ordered_list:
            item = {key: i.get(key) or '' for key in FIELDS_TO_EXPORT}
            self.exporter.export_item(item)

        # Ending the export to file
        self.exporter.finish_exporting()
        self.file.close()

    def process_item(self, item, spider):
        self.list_items.append(item)
        return item


class RemoveIgnoredKeywordsPipeline:
    """ Remove jobs ads with 'ads_title' containing one of the words in the IGNORED_KEYWORDS """
    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        for keyword in JOB_TITLE_IGNORE_KEYWORDS:
            if re.search(keyword, adapter['ads_title'], re.IGNORECASE):
                raise DropItem(f"'Postdoc' item found: {item!r}")
        return item


class DeDuplicatesPipeline:
    """ Remove duplication based on the ID of each ads for the specific jobs board """

    def __init__(self):
        self.ids_seen = set()

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        if adapter.get('ads_job_code'):
            if adapter['ads_job_code'] in self.ids_seen:
                raise DropItem(f"Duplicate item found: {item!r}")
            self.ids_seen.add(adapter['ads_job_code'])
        return item


class ChempostingcanadaSpider(XMLFeedSpider):
    name = 'chempostingscanada.blogspot.com'
    allowed_domains = ['chempostingscanada.blogspot.com']
    start_urls = ['http://chempostingscanada.blogspot.com/feeds/posts/default']
    namespaces = [('x', 'http://www.w3.org/2005/Atom'),
                  ('openSearch', 'http://a9.com/-/spec/opensearchrss/1.0/'),
                  ('blogger', 'http://schemas.google.com/blogger/2008'),
                  ('georss', 'http://www.georss.org/georss'),
                  ('gd', "http://schemas.google.com/g/2005"),
                  ('thr', 'http://purl.org/syndication/thread/1.0')]
    iterator = 'iternodes'  # This is actually unnecessary, since it's the default value
    itertag = 'entry'

    def parse_node(self, response, node):
        # self.logger.info('Hi, this is a <%s> node!: %s', self.itertag, ''.join(node.getall()))

        item = JobItem()

        posted_date = node.xpath('.//x:published/text()').get()
        #  Convert to datetime format mm/dd/yyyy
        posted_date = datetime.fromisoformat(posted_date)
        timezone_info = posted_date.tzinfo
        posted_date_string = posted_date.strftime('%m/%d/%Y')
        now = datetime.now(tz=timezone_info)
        is_posted_in_the_past_five_days = (now - posted_date).days <= 10

        if not is_posted_in_the_past_five_days:
            return

        item['posted_date'] = posted_date_string

        title = node.xpath('.//x:title/text()').get()
        school, _, title = title.partition(':')
        school, title = map(str.strip, [school, title])
        item['ads_title'] = title
        item['canada'] = True

        details_url = node.xpath('.//x:link[@rel="alternate"]/@href').get()
        ads_source = f'=hyperlink("{details_url}","ChemPostingCanada")'
        item['ads_source'] = ads_source

        recruiter = f'=hyperlink("{details_url}","{school}")'
        item['school'] = recruiter

        self.logger.info(f'{item=}')
        return item


if __name__ == '__main__':
    # Remove the result file if exists
    THIS_SPIDER_RESULT_FILE.unlink(missing_ok=True)

    settings = {
        'USER_AGENT': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.96 Safari/537.36',
        # 'USER_AGENT': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.87 Safari/537.36',
        # 'HTTPCACHE_ENABLED': True,
        # 'DEFAULT_REQUEST_HEADERS': {
        #   'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        #   'Accept-Language': 'en'
        # },
        'CSV_EXPORT_FILE': THIS_SPIDER_RESULT_FILE,
        'ITEM_PIPELINES': {
            '__main__.RemoveIgnoredKeywordsPipeline': 100,
            '__main__.DeDuplicatesPipeline': 800,
            '__main__.CsvWriteLatestToOldest': 900,
            },
        # 'FEEDS': {
        #     Path(THIS_SPIDER_RESULT_FILE): {
        #         'format': 'csv',
        #         'fields': FIELDS_TO_EXPORT,
        #         'overwrite': True,
        #         'store_empty': False,
        #     },
        # },
        'LOG_LEVEL': 'INFO',
        # 'ROBOTSTXT_OBEY': False,
    }

    process = CrawlerProcess(settings=settings)
    process.crawl(ChempostingcanadaSpider)
    process.start()

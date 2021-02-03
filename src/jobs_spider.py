# quote_spiders.py
import re
from pathlib import Path
from datetime import datetime

import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.item import Item, Field
# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem
from scrapy.exporters import CsvItemExporter

CURRENT_FILEPATH = Path(__file__).resolve().parent

RESULT_FILE = CURRENT_FILEPATH / 'jobs.csv'

JOB_TITLE_IGNORE_KEYWORDS = ['post-doc', 'postdoc', 'scientist']

FIELDS_TO_EXPORT = ['posted_date', 'priority_date', 'category',
            'school', 'department', 'specialization',
            'rank', 'city', 'state', 'canada',
            'current_status', 'comments1', 'comments2',
            'ads_title', 'ads_source', 'ads_job_code'
            ]

class CsvWriteOldestToLatest(object):

    def open_spider(self, spider):
        self.list_items = []
        self.file = open(Path(RESULT_FILE), 'wb')

        # Creating a FanItemExporter object and initiating export
        self.exporter = CsvItemExporter(self.file, fields_to_export=FIELDS_TO_EXPORT)
        # self.exporter = CsvItemExporter(self.file)
        self.exporter.start_exporting()

    def close_spider(self, spider):
        ordered_list = sorted(self.list_items, key=lambda i: i['posted_date'])

        for i in ordered_list:
            item = {key: i.get(key) or '' for key in FIELDS_TO_EXPORT}
            self.exporter.export_item(item)

        # Ending the export to file
        self.exporter.finish_exporting()
        self.file.close()

    def process_item(self, item, spider):
        self.list_items.append(item)
        return item


class RemovePostdocPipeline:
    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        # if adapter['id'] in self.ids_seen:
        for keyword in JOB_TITLE_IGNORE_KEYWORDS:
            if re.search(keyword, adapter['ads_title'], re.IGNORECASE):
                raise DropItem(f"'Postdoc' item found: {item!r}")
        return item


class DeDuplicatesPipeline:

    def __init__(self):
        self.ids_seen = set()

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        # if adapter['id'] in self.ids_seen:
        if adapter['ads_job_code'] in self.ids_seen:
            raise DropItem(f"Duplicate item found: {item!r}")
        self.ids_seen.add(adapter['ads_job_code'])
        return item


class HigheredjobsItem(Item):
    posted_date = Field()
    school = Field()
    department = Field()
    city = Field()
    state = Field()
    ads_title = Field()
    ads_source = Field()
    ads_job_code = Field()
    rank = Field()
    specialization = Field()


class JobsHigheredjobsSpider(scrapy.Spider):
    name = 'jobs_higheredjobs'
    allowed_domains = ['higheredjobs.com']
    start_urls = ['https://www.higheredjobs.com/faculty/search.cfm?JobCat=101&StartRow=-1&SortBy=1&NumJobs=25&filterby=&filterptype=1&filtercountry=38&filtercountry=226&CatType=']
    base_url = 'https://www.higheredjobs.com/faculty/'

    def parse(self, response):
        # print(f'{response.body=}')
        # print(f'{response.meta=}')
        # print(f'{response.headers=}')
        jobs = response.css('.row.record')
        for job in jobs:
            title = job.xpath('.//a/text()').get().strip()
            link = self.base_url + job.xpath('.//a/@href').get()

            ads_job_code = re.findall(r'(?<=JobCode=).*(?=&)', link)[0]
            all_text = job.xpath('.//text()').extract()

            # Remove all whitespace
            [_, school, location, department, posted_date ] = [word.strip() for word in all_text if re.search(r'\S',word)]
            posted_date = datetime.strptime(re.findall(r'\d{2}/\d{2}/\d{2}', posted_date)[0], '%m/%d/%y')
            school = f'=hyperlink("{link}","{school}")'
            city, _, state = location.partition(',')
            city, state = map(str.strip, [city, state])
            ads_source = f'=hyperlink("{self.start_urls[0]}","HigherEdJobs")'

            # Get the ranking
            rank = re.findall(r'assist|assoc', title, re.IGNORECASE)
            rank = '/'.join(word.lower().replace('assist', 'asst')
                            for word in rank)

            # Get specialization
            specialization = re.findall(r'org\w*|anal\w*|inorg\w*|bio\w*|physic\w*|polymer\w*', title, re.IGNORECASE)
            specialization = ', '.join(specialization)

            item = HigheredjobsItem({
                'posted_date': posted_date.strftime('%m/%d/%Y'),
                'school': school,
                'department': department,
                'city': city,
                'state': state,
                'ads_title': title,
                'ads_source': ads_source,
                'ads_job_code': ads_job_code,
                'rank': rank,
                'specialization': specialization,
            })
            yield item


if __name__ == '__main__':
    settings = {
        'USER_AGENT': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.96 Safari/537.36',
        # 'USER_AGENT': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.87 Safari/537.36',
        # 'BOT_NAME': 'Jobs-list-check',
        # 'HTTPCACHE_ENABLED': True,
        # 'DEFAULT_REQUEST_HEADERS': {
        #   'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        #   'Accept-Language': 'en'
        # },
        'ITEM_PIPELINES': {
            '__main__.RemovePostdocPipeline': 100,
            '__main__.DeDuplicatesPipeline': 800,
            # '__main__.CsvWriteOldestToLatest': 900,
            },
        'FEEDS': {
            Path(RESULT_FILE): {
                'format': 'csv',
                'fields': ['posted_date', 'priority_date', 'category',
                           'school', 'department', 'specialization',
                           'rank', 'city', 'state', 'canada',
                           'current_status', 'comments1', 'comments2',
                           'ads_title', 'ads_source', 'ads_job_code'
                           ],
                'overwrite': True,
                'store_empty': False,
            },
        },
        'LOG_LEVEL': 'INFO',
        # 'ROBOTSTXT_OBEY': False,
    }

    process = CrawlerProcess(settings=settings)
    process.crawl(JobsHigheredjobsSpider)
    process.start()

    from write_to_sheet import write_csv_to_google_sheet
    write_csv_to_google_sheet(RESULT_FILE)

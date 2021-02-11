# quote_spiders.py
import re
from datetime import datetime
from pathlib import Path

import scrapy
# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from scrapy.crawler import CrawlerProcess
from scrapy.exceptions import DropItem
from scrapy.exporters import CsvItemExporter

from items import JobItem


CURRENT_FILEPATH = Path(__file__).resolve().parent
DATA_FOLDER = CURRENT_FILEPATH.parent / 'data'
DATA_FOLDER.mkdir(exist_ok=True)
THIS_SPIDER_RESULT_FILE = DATA_FOLDER / 'higheredjobs_jobs.csv'

JOB_TITLE_IGNORE_KEYWORDS = ['post-doc', 'postdoc', 'post doc', 'scientist']

FIELDS_TO_EXPORT = ['ads_title', 'posted_date', 'priority_date', 'category',
                    'school', 'department', 'specialization',
                    'rank', 'city', 'state', 'canada',
                    'current_status', 'comments1', 'comments2',
                    'ads_source', 'ads_job_code'
                    ]


class CsvWriteLatestToOldest(object):
    """ Write to CSV file in latest to oldest order of 'posted_date' """
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
                raise DropItem(f"'{keyword}' item found: {item!r}")
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


class JobsHigheredjobsSpider(scrapy.Spider):
    name = 'jobs_higheredjobs'
    allowed_domains = ['higheredjobs.com']
    start_urls = ['https://www.higheredjobs.com/faculty/search.cfm?JobCat=101&StartRow=-1&SortBy=1&NumJobs=25&filterby=&filterptype=1&filtercountry=38&filtercountry=226&CatType=']
    base_url = 'https://www.higheredjobs.com/faculty/'

    def parse(self, response):
        jobs = response.css('.row.record')
        is_posted_in_the_past_five_days = True
        for job in jobs:
            title = job.xpath('.//a/text()').get().strip()
            details_url = response.urljoin(job.xpath('.//a/@href').get())
            ads_job_code = re.findall(r'(?<=JobCode=).*(?=&)', details_url)[0]
            all_text = job.xpath('.//text()').extract()
            # print(f'{all_text=}')
            # print([word.strip() for word in all_text if re.search(r'\S', word)])
            [_, school, location, department, *posted_date] = [word.strip()
                                                               for word in all_text
                                                               if re.search(r'\S', word)]
            posted_date = datetime.strptime(re.findall(r'\d{2}/\d{2}/\d{2}', posted_date[0])[0],
                                            '%m/%d/%y')
            is_posted_in_the_past_five_days = ((datetime.now() - posted_date).days <= 5)

            city, _, state = location.partition(',')
            city, state = map(str.strip, [city, state])
            ads_source = f'=hyperlink("{details_url}","HigherEdJobs")'

            # Get the ranking
            rank = re.findall(r'assist|assoc', title, re.IGNORECASE)
            rank = '/'.join(word.lower().replace('assist', 'asst')
                            for word in rank)

            # Get specialization
            specialization = re.findall(r'org\w*|anal\w*|inorg\w*|bio\w*|physic\w*|polymer\w*', title, re.IGNORECASE)
            specialization = ', '.join(specialization)

            cb_kwargs = {
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
            }

            # Pass the callback function arguments with 'cb_kwargs': https://docs.scrapy.org/en/latest/topics/request-response.html?highlight=cb_kwargs#scrapy.http.Request.cb_kwargs
            if is_posted_in_the_past_five_days:
                yield scrapy.Request(url=details_url,
                                     cb_kwargs=cb_kwargs,
                                     callback=self.parse_ads)

        # Find next page url if exists:
        next_page_partial_url = response.xpath('.//a[.//img[not(contains(@class, "disabled")) and contains(@src, "right.gif")]]/@href').get()
        # print(f'{next_page_partial_url=}')

        if next_page_partial_url and is_posted_in_the_past_five_days:
            next_page_url = response.urljoin(next_page_partial_url)
            # print(f'{next_page_url=}')
            yield scrapy.Request(url=next_page_url, callback=self.parse)

    def parse_ads(self, response, **cb_kwargs):
        online_application_title_field = response.xpath(
            './/*[@id="jobApplyInfo"]//*[contains(@class, "field-label")][contains(normalize-space(text()), "Online App. Form")]'
        )
        online_application_url = online_application_title_field.xpath('./following-sibling::div[1]/a/@data-orig-href').get()

        # Update the school field to embed the link to the online app if exists (following Chemjobber List format)
        application_url = online_application_url or response.url
        cb_kwargs['school'] = f'=hyperlink("{application_url}","{cb_kwargs["school"]}")'
        # print(f'{cb_kwargs=}')
        yield JobItem(cb_kwargs)


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
        'LOG_LEVEL': 'DEBUG',
        # 'ROBOTSTXT_OBEY': False,
    }

    process = CrawlerProcess(settings=settings)
    process.crawl(JobsHigheredjobsSpider)
    process.start()

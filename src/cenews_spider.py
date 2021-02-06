# quote_spiders.py
import re
from pathlib import Path, PurePath
from datetime import datetime
import urllib

import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.item import Item, Field
# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem
from scrapy.exporters import CsvItemExporter

CURRENT_FILEPATH = Path(__file__).resolve().parent

THIS_SPIDER_RESULT_FILE = CURRENT_FILEPATH / 'cenew_jobs.csv'

JOB_TITLE_IGNORE_KEYWORDS = ['post-doc', 'postdoc', 'post doc', 'scientist']

FIELDS_TO_EXPORT = ['posted_date', 'priority_date', 'category',
            'school', 'department', 'specialization',
            'rank', 'city', 'state', 'canada',
            'current_status', 'comments1', 'comments2',
            'ads_title', 'ads_source', 'ads_job_code'
            ]


class CsvWriteLatestToOldest:

    def __init__(self, csv_export_file):
        self.csv_export_file = csv_export_file

    @classmethod
    def from_crawler(cls, crawler):
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
        ordered_list = sorted(self.list_items, key=lambda i: i['posted_date'], reverse=True)

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


class JobItem(Item):
    posted_date = Field()
    priority_date = Field()
    school = Field()
    department = Field()
    city = Field()
    state = Field()
    ads_title = Field()
    ads_source = Field()
    ads_job_code = Field()
    rank = Field()
    specialization = Field()


class ChemicalEngineeringNewsSpider(scrapy.Spider):
    name = 'chemical_engineering_news_job'
    allowed_domains = ['chemistryjobs.acs.org']
    start_urls = ['https://chemistryjobs.acs.org/jobs/full-time/north-america/']
    base_url = 'https://chemistryjobs.acs.org/'
    # handle_httpstatus_list = [301, 302]

    def parse(self, response):
        # print(f'{response.body=}')
        # print(f'{response.meta=}')
        # print(f'{response.headers=}')

        # Get all the jobs listing
        jobs = response.css('.lister__item .lister__details')

        # # Pick only those ads that has green badge of 'new' on the top right corner:
        # # For 'C&E News' magazine, 'new' is for job posted in the past 2 days:
        # # Use C&E News own 'filter' of ads posted in the past 2 days (have green badge say 'New' in the top left corner of each ads listing)
        # jobs = response.xpath('//*[contains(@class, "lister__item")][.//*[contains(@class, "badge--green")]]//*[contains(@class, "lister__details")]')

        for job in jobs:
            title = job.xpath('.//*[contains(@class, "lister__header")]//a//text()').get().strip()
            # print(f'{title=}')
            location = job.xpath('.//*[contains(@class, "lister__meta-item--location")]//text()').get()
            city, _, state = location.partition(',')
            city, state = map(str.strip, [city, state])
            # print(f'{location=}')

            recruiter = job.xpath('.//*[contains(@class, "lister__meta-item--recruiter")]//text()').get()
            # print(f'{recruiter=}')

            details_url = response.urljoin(job.xpath('.//following-sibling::*[contains(@class, "lister__footer")]//*[contains(@class, "lister__view-details")]//a/@href').get().strip())

            ads_job_code = re.findall(r'(?<=/job/).*?(?=/)', details_url)[0]
            # print(f'{ads_job_code=}')

            # all_text = job.xpath('.//text()').extract()

            # # Remove all whitespace
            # # print(f'{all_text=}')
            # # print([word.strip() for word in all_text if re.search(r'\S', word)])
            # [_, school, location, department, *posted_date] = [word.strip() for word in all_text if re.search(r'\S', word)]

            ads_source = f'=hyperlink("{details_url}","C&ENJobs")'

            # Get the ranking
            rank = re.findall(r'assist|assoc', title, re.IGNORECASE)
            rank = '/'.join(word.lower().replace('assist', 'asst')
                            for word in rank)

            # # Get specialization
            # specialization = re.findall(r'org\w*|anal\w*|inorg\w*|bio\w*|physic\w*|polymer\w*', title, re.IGNORECASE)
            # specialization = ', '.join(specialization)

            cb_kwargs = {
                'school': recruiter,
                # 'department': department,
                'city': city,
                'state': state,
                'ads_title': title,
                'ads_source': ads_source,
                'ads_job_code': ads_job_code,
                'rank': rank,
                # 'specialization': specialization,
            }
            # yield JobItem(cb_kwargs)

            # Pass the callback function arguments with 'cb_kwargs': https://docs.scrapy.org/en/latest/topics/request-response.html?highlight=cb_kwargs#scrapy.http.Request.cb_kwargs
            yield scrapy.Request(url=details_url, cb_kwargs=cb_kwargs, callback=self.parse_ads)

        # Find next page url if exists:
        next_page_partial_url = response.xpath('//*[not(contains(@class, "paginator__items"))][contains(@class, "paginator__item")][.//*[contains(@rel, "next")]]//a/@href').get()
        # print(f'{next_page_partial_url=}')
        if next_page_partial_url:
            next_page_url = response.urljoin(next_page_partial_url)
            # print(f'{next_page_url=}')
            yield scrapy.Request(url=next_page_url, callback=self.parse)

    def parse_ads(self, response, **cb_kwargs):
        # Get the text
        posted_date = ''.join(response.css('.job-detail-description__posted-date > *:last-child *::text').getall()).strip()
        #  Convert to datetime format mm/dd/yyyy
        posted_date = datetime.strptime(posted_date, '%b %d, %Y')
        posted_date_string = posted_date.strftime('%m/%d/%Y')

        priority_date = ''.join(response.css('.job-detail-description__end-date > *:last-child *::text').getall()).strip()
        priority_date = datetime.strptime(priority_date, '%b %d, %Y').strftime('%m/%d/%Y')

        specialization_text_list = response.css('.job-detail-description__category-Fieldofspecialization > *:last-child *::text').getall()
        specialization = re.sub(r'\s{2,}', '', ''.join(specialization_text_list))

        cb_kwargs.update({'posted_date': posted_date_string,
                          'priority_date': priority_date,
                          'specialization': specialization})
        # yield JobItem(cb_kwargs)

        posted_in_the_past_five_days = (datetime.now() - posted_date).days <= 5
        # Update the school field to embed the link to the online app if exists (following Chemjobber List format)
        # scrapy `.attrib` is also available on SelectorList directly; it returns attributes for the first matching element:returns attributes for the first matching element:
        # https://docs.scrapy.org/en/latest/topics/selectors.html#using-selectors
        apply_button_partial_url = response.css('a.button--apply').attrib['href']
        if apply_button_partial_url and posted_in_the_past_five_days:
            apply_button_url = response.urljoin(apply_button_partial_url) + '&Action=Cancel'
            # print(f'{apply_button_url=}')

            yield scrapy.Request(url=apply_button_url,
                                 callback=self.parse_redirect_application_url,
                                 cb_kwargs=cb_kwargs)

            # online_application_url = scrapy.Request(url=response.urljoin(apply_button_partial_url))
            # online_application_url = urllib.request.urlopen(apply_button_url).geturl()
            # print(f'{online_application_url.response.url=}')


    def parse_redirect_application_url(self, response, **cb_kwargs):
        # test = response.css('meta[http-equiv*="refresh"]').get()
        # test = response.css('meta').getall()
        # print(f'{test=}')
        application_url = response.url or response.request.url
        # print(f'{application_url=}')
        cb_kwargs['school'] = f'=hyperlink("{application_url}","{cb_kwargs["school"]}")'
        # print(f'{cb_kwargs=}')
        yield JobItem(cb_kwargs)


if __name__ == '__main__':
    # Remove the result file if exists
    THIS_SPIDER_RESULT_FILE.unlink(missing_ok=True)

    settings = {
        'USER_AGENT': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.96 Safari/537.36',
        # 'USER_AGENT': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.87 Safari/537.36',
        # 'BOT_NAME': 'Jobs-list-check',
        # 'HTTPCACHE_ENABLED': True,
        # 'DEFAULT_REQUEST_HEADERS': {
        #   'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        #   'Accept-Language': 'en'
        # },
        'CSV_EXPORT_FILE': THIS_SPIDER_RESULT_FILE,
        'ITEM_PIPELINES': {
            '__main__.RemovePostdocPipeline': 100,
            '__main__.DeDuplicatesPipeline': 800,
            '__main__.CsvWriteLatestToOldest': 900,
            },
        # 'FEEDS': {
        #     Path(RESULT_FILE): {
        #         'format': 'csv',
        #         'fields': ['posted_date', 'priority_date', 'category',
        #                    'school', 'department', 'specialization',
        #                    'rank', 'city', 'state', 'canada',
        #                    'current_status', 'comments1', 'comments2',
        #                    'ads_title', 'ads_source', 'ads_job_code'
        #                    ],
        #         'overwrite': True,
        #         'store_empty': False,
        #     },
        # },
        'LOG_LEVEL': 'INFO',
        # 'ROBOTSTXT_OBEY': False,
    }

    process = CrawlerProcess(settings=settings)
    process.crawl(ChemicalEngineeringNewsSpider)
    process.start()

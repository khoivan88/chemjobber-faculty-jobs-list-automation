import re
import json
from datetime import datetime, timezone
from pathlib import Path, PurePath

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
THIS_SPIDER_RESULT_FILE = DATA_FOLDER / 'cenew_jobs.csv'

JOB_TITLE_IGNORE_KEYWORDS = ['post-doc', 'postdoc', 'post doc', 'scientist']

FIELDS_TO_EXPORT = ['ads_title', 'posted_date', 'priority_date', 'category',
                    'school', 'department', 'specialization',
                    'rank', 'city', 'state', 'canada',
                    'current_status', 'comments1', 'comments2',
                    'ads_source', 'ads_job_code'
                    ]


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


class ChemicalEngineeringNewsSpider(scrapy.Spider):
    name = 'chemical_engineering_news_job'
    allowed_domains = ['chemistryjobs.acs.org']
    start_urls = ['https://chemistryjobs.acs.org/jobs/full-time/north-america/']
    base_url = 'https://chemistryjobs.acs.org/'
    # handle_httpstatus_list = [301, 302]

    def parse(self, response):
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
            is_in_canada = 'yes' if (re.search(r'\(CA\)', state)) else None

            # print(f'{location=}')
            recruiter = job.xpath('.//*[contains(@class, "lister__meta-item--recruiter")]//text()').get()
            # print(f'{recruiter=}')
            details_url = response.urljoin(job.xpath('.//following-sibling::*[contains(@class, "lister__footer")]//*[contains(@class, "lister__view-details")]//a/@href').get().strip())
            ads_job_code = re.findall(r'(?<=/job/).*?(?=/)', details_url)[0]
            # print(f'{ads_job_code=}')
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
                'canada': is_in_canada,
            }
            # yield JobItem(cb_kwargs)

            # Pass the callback function arguments with 'cb_kwargs': https://docs.scrapy.org/en/latest/topics/request-response.html?highlight=cb_kwargs#scrapy.http.Request.cb_kwargs
            yield scrapy.Request(url=details_url,
                                 cb_kwargs=cb_kwargs,
                                 callback=self.parse_ads)

        # Find next page url if exists:
        next_page_partial_url = response.xpath('//*[not(contains(@class, "paginator__items"))][contains(@class, "paginator__item")][.//*[contains(@rel, "next")]]//a/@href').get()
        # print(f'{next_page_partial_url=}')
        if next_page_partial_url:
            next_page_url = response.urljoin(next_page_partial_url)
            # print(f'{next_page_url=}')
            yield scrapy.Request(url=next_page_url, callback=self.parse)

    def parse_ads(self, response, **cb_kwargs):
        data_layer_string = response.xpath('//script[contains(., "DataLayer")]/text()').get()
        data_layer = re.search(r'.*(\{.+\})', data_layer_string, re.MULTILINE|re.DOTALL)
        # print(f'{data_layer=}')
        if data_layer:
            data = json.loads(data_layer.group(1))
            # print(f'{data=}')

        detail_data_layer_string = response.css('script[type="application/ld+json"]::text').get()
        if detail_data_layer_string:
            detail_data = json.loads(detail_data_layer_string)
            # print(f'{detail_data=}')
            
        # Get the text
        # posted_date = ''.join(response.css('.job-detail-description__posted-date > *:last-child *::text').getall()).strip()
        posted_date = response.css('meta[property="og:article:published_time"]').attrib.get('content')
        #  Convert to datetime format mm/dd/yyyy
        # posted_date = datetime.strptime(posted_date, '%b %d, %Y')
        posted_date_obj = datetime.fromisoformat(posted_date)
        posted_date_string = posted_date_obj.strftime('%m/%d/%Y')

        # priority_date = ''.join(response.css('.job-detail-description__end-date > *:last-child *::text').getall()).strip()
        # priority_date = datetime.strptime(priority_date, '%b %d, %Y').strftime('%m/%d/%Y')
        priority_date = response.css('meta[property="og:article:expiration_time"]').attrib.get('content')
        priority_date = datetime.fromisoformat(priority_date).strftime('%m/%d/%Y')

        if data_layer:
            specialization = data.get('Field of specialization-AllTerms')
        else: 
            specialization_text_list = response.css('.job-detail-description__category-Fieldofspecialization > *:last-child *::text').getall()
            specialization = re.sub(r'\s{2,}', '', ''.join(specialization_text_list))

        # job_description = ' '.join(word.strip()
        #                            for word in (response.css('.job-description *::text').getall())
        #                            if re.search(r'\S', word))
        if detail_data_layer_string:
            job_description = detail_data.get('description')
        if not job_description:
            job_description = ' '.join(word.strip()
                                   for word in (response.css('.job-description *::text').getall())
                                   if re.search(r'\S', word))
        # print(f'{job_description=}')

        # Get the ranking (using the job description)
        rank = re.findall(r'Assistant\b|Associate\b|Full\s', job_description)
        rank_text = '/'.join(word.lower().replace('assistant', 'asst').replace('associate', 'assoc')
                             for word in rank)
        # print(f'{rank_text=}')
        cb_kwargs['rank'] = rank_text or cb_kwargs['rank']

        tenure_type = re.search(r'\S*tenure\S*', job_description, re.IGNORECASE)
        # print(f'{tenure_type=}')
        comments1 = tenure_type[0] if tenure_type else None

        cb_kwargs.update({'posted_date': posted_date_string,
                          'priority_date': priority_date,
                          'specialization': specialization,
                          'comments1': comments1})
        # yield JobItem(cb_kwargs)

        is_posted_in_the_past_five_days = (datetime.now(tz=timezone.utc) - posted_date_obj).days <= 5
        # Update the school field to embed the link to the online app if exists (following Chemjobber List format)
        # scrapy `.attrib` is also available on SelectorList directly; it returns attributes for the first matching element:returns attributes for the first matching element:
        # https://docs.scrapy.org/en/latest/topics/selectors.html#using-selectors
        # apply_button_partial_url = response.css('a.button--apply').attrib['href']
        apply_button_partial_url = response.css('a[data-hook="apply-button"]').attrib['href']
        if apply_button_partial_url and is_posted_in_the_past_five_days:
            apply_button_url = response.urljoin(apply_button_partial_url) + '&Action=Cancel'
            # print(f'{apply_button_url=}')

            yield scrapy.Request(url=apply_button_url,
                                 callback=self.parse_redirect_application_url,
                                 cb_kwargs=cb_kwargs)

    def parse_redirect_application_url(self, response, **cb_kwargs):
        """ Get the redirect url to the application url """
        application_url = response.url or response.request.url
        # print(f'{application_url=}')
        cb_kwargs['school'] = f'=hyperlink("{application_url}","{cb_kwargs["school"]}")'
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
        'LOG_LEVEL': 'INFO',
        # 'ROBOTSTXT_OBEY': False,
    }

    process = CrawlerProcess(settings=settings)
    process.crawl(ChemicalEngineeringNewsSpider)
    process.start()

from pathlib import Path
import scrapy
from scrapy.crawler import CrawlerProcess

from higheredjobs_spider import JobsHigheredjobsSpider
from write_to_sheet import write_csv_to_google_sheet


CURRENT_FILEPATH = Path(__file__).resolve().parent
RESULT_FILE = CURRENT_FILEPATH / 'jobs.csv'

JOB_TITLE_IGNORE_KEYWORDS = ['post-doc', 'postdoc', 'scientist']

FIELDS_TO_EXPORT = ['posted_date', 'priority_date', 'category',
                    'school', 'department', 'specialization',
                    'rank', 'city', 'state', 'canada',
                    'current_status', 'comments1', 'comments2',
                    'ads_title', 'ads_source', 'ads_job_code'
                    ]


if __name__ == '__main__':
    settings = {
        'USER_AGENT': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.96 Safari/537.36',
        # 'USER_AGENT': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.87 Safari/537.36',
        # 'BOT_NAME': 'Jobs-list-check',
        'HTTPCACHE_ENABLED': True,
        # 'DEFAULT_REQUEST_HEADERS': {
        #   'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        #   'Accept-Language': 'en'
        # },
        'ITEM_PIPELINES': {
            'higheredjobs_spider.RemovePostdocPipeline': 100,
            'higheredjobs_spider.DeDuplicatesPipeline': 800,
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

    # write_csv_to_google_sheet(RESULT_FILE)

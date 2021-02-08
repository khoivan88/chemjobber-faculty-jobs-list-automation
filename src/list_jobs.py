import csv
from pathlib import Path, PurePath
from typing import Sequence

from scrapy.crawler import CrawlerProcess

from cenews_spider import ChemicalEngineeringNewsSpider
from higheredjobs_spider import JobsHigheredjobsSpider
from chroniclehighered_spider import ChronicalHigherEducationSpider
from write_to_sheet import write_csv_to_google_sheet


CURRENT_FILEPATH = Path(__file__).resolve().parent
DATA_FOLDER = CURRENT_FILEPATH.parent / 'data'
DATA_FOLDER.mkdir(exist_ok=True)
RESULT_FILE = DATA_FOLDER / 'jobs.csv'

JOB_TITLE_IGNORE_KEYWORDS = ['post-doc', 'postdoc', 'scientist']

FIELDS_TO_EXPORT = ['ads_title', 'posted_date', 'priority_date', 'category',
                    'school', 'department', 'specialization',
                    'rank', 'city', 'state', 'canada',
                    'current_status', 'comments1', 'comments2',
                    'ads_source', 'ads_job_code'
                    ]


def sort_csv(file: PurePath, fieldnames: Sequence, sort_by: str, reverse: bool = False):
    """Sort a csv file by the 'sort_by' column name

    Parameters
    ----------
    file : PurePath
        csv file to be sorted
    fieldnames : Sequence
        The header list of strings for the csv name
    sort_by : str
        The name of the column that to be sorted by
    reverse : bool, optional
        Setting for reversed order, by default False
    """
    with open(file, 'r') as f_in:
        dict_reader = csv.DictReader(f_in, fieldnames=fieldnames)
        data = list(dict_reader)

    sorted_data = sorted(data, key=lambda i: i[sort_by], reverse=reverse)

    with open(file, 'w') as f_out:
        dict_writer = csv.DictWriter(f_out, fieldnames=fieldnames)
        dict_writer.writeheader()
        dict_writer.writerows(sorted_data)


if __name__ == '__main__':
    # Remove the result file if exists
    RESULT_FILE.unlink(missing_ok=True)

    settings = {
        'USER_AGENT': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.96 Safari/537.36',
        # 'USER_AGENT': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.87 Safari/537.36',
        # 'BOT_NAME': 'Jobs-list-check',
        # 'HTTPCACHE_ENABLED': True,
        # 'DEFAULT_REQUEST_HEADERS': {
        #   'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        #   'Accept-Language': 'en'
        # },
        'CSV_EXPORT_FILE': RESULT_FILE,
        'ITEM_PIPELINES': {
            # 'higheredjobs_spider.RemoveIgnoredKeywordsPipeline': 1,
            # 'higheredjobs_spider.DeDuplicatesPipeline': 2,
            # 'higheredjobs_spider.CsvWriteLatestToOldest': 3,
            'cenews_spider.RemoveIgnoredKeywordsPipeline': 4,
            'cenews_spider.DeDuplicatesPipeline': 5,
            # 'cenews_spider.CsvWriteLatestToOldest': 6,
            },
        'FEEDS': {
            Path(RESULT_FILE): {
                'format': 'csv',
                'fields': FIELDS_TO_EXPORT,
                'overwrite': False,
                'store_empty': False,
                'item_export_kwargs': {
                    'include_headers_line': False,
                }
            },
        },
        'LOG_LEVEL': 'INFO',
        # 'ROBOTSTXT_OBEY': False,
    }

    process = CrawlerProcess(settings=settings)
    process.crawl(JobsHigheredjobsSpider)
    process.crawl(ChemicalEngineeringNewsSpider)
    process.crawl(ChronicalHigherEducationSpider)
    process.start()

    # Sort the resulting csv file
    sort_csv(file=RESULT_FILE, fieldnames=FIELDS_TO_EXPORT,
             sort_by='posted_date', reverse=True)

    # Write csv file to google sheet
    write_csv_to_google_sheet(RESULT_FILE)

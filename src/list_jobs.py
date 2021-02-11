import csv
import re
from collections import Counter
from pathlib import Path, PurePath
from typing import Dict, List, Sequence

from furl import furl
from scrapy.crawler import CrawlerProcess

from cenews_spider import ChemicalEngineeringNewsSpider
from chroniclehighered_spider import ChronicalHigherEducationSpider
from higheredjobs_spider import JobsHigheredjobsSpider
from chempostingcanada_spider import ChempostingcanadaSpider
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


def process_csv(file: PurePath, fieldnames: Sequence, sort_by: str, reverse: bool = False) -> None:
    """ Remove duplicated row & Sort a csv file by the 'sort_by' column name

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

    deduplicated_data = remove_duplicate(data)
    # print(f'{len(deduplicated_data)=}')

    sorted_data = sorted(deduplicated_data, key=lambda i: i[sort_by], reverse=reverse)

    with open(file, 'w') as f_out:
        dict_writer = csv.DictWriter(f_out, fieldnames=fieldnames)
        dict_writer.writeheader()
        dict_writer.writerows(sorted_data)


def remove_duplicate(data: List[Dict[str, str]]) -> List[Dict[str, str]]:
    """Remove duplicated row based on 'ads_title' and then the application url

    Duplicated is first considered based on the 'ads_title' then the url in the 'school' key.
    url is first processed to remove query 'source' as well as scheme (e.g. 'http' or 'https')

    Parameters
    ----------
    data : List[Dict[str, str]]
        The list of csv rows without header, should be passed in with csv.DictReader

    Returns
    -------
    List[Dict[str, str]]
        Remove List with similar structure but duplicated removed
    """
    count_result = Counter(row['ads_title'] for row in data)
    # print(f'{count_result=}')

    # Get all rows with 'ads_title' count more than 1
    duplicate_titles = [title for title, count in count_result.items() if count > 1]
    # print(f'{duplicate_titles=}')

    # Add all non-duplicated into the result list
    result = [row for row in data if row['ads_title'] not in duplicate_titles]
    # print(f'{len(result)=}')

    for title in duplicate_titles:
        duplicated_rows = [row for row in data if row['ads_title'] == title]
        # print(f'{duplicated_rows=}')

        existing_info = set()
        for row in duplicated_rows:
            url, school_name = re.findall(r'\"(.*?)\"', row['school'])
            # Remove query 'source' since some url is like this:
            # 'https://embryriddle.wd1.myworkdayjobs.com/en-US/External/job/Daytona-Beach-FL/Non-Tenure-Track-Faculty-Position-in-Chemistry--Daytona-Beach-Campus-_R300364?source=HigherEdJobs'
            url = furl(url).remove(query=['source']).url
            # Need to remove scheme ('http' or 'https'):
            url = re.sub(r'https?://', '', url)
            if (url, school_name) not in existing_info:
                existing_info.add((url, school_name))
                result.append(row)
            # print(f'{existing_info=}')

    return result


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
    process.crawl(ChempostingcanadaSpider)
    process.start()

    # Sort the resulting csv file
    process_csv(file=RESULT_FILE, fieldnames=FIELDS_TO_EXPORT,
                sort_by='posted_date', reverse=True)

    # Write csv file to google sheet
    write_csv_to_google_sheet(RESULT_FILE)

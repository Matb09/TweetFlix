# -*- coding: utf-8 -*-
import logging
from utils import search_recent, extract_info, save_rawdata_gcs, delete_data_period
from datetime import datetime, timedelta
from google.cloud import bigquery
import argparse
import pytz

logger = logging.getLogger(__name__)

def main():

    # parsing airflow possibile daterange, default 6
    parser = argparse.ArgumentParser()
    parser.add_argument("--start_time", "-s")
    parser.add_argument("--end_time", "-e")
    parser.add_argument("--disable_override", "-d")
    options = vars(parser.parse_args())

    disable_override = options['disable_override']
    start_time = datetime.strptime(options['start_time'], '%Y-%m-%d').date() if options['start_time'] else datetime.now(tz=pytz.UTC).date() - timedelta(days=2)
    end_time = datetime.strptime(options['end_time'], '%Y-%m-%d').date() if options['end_time'] else datetime.now(tz=pytz.UTC).date()

    logger.info(f"Start from {start_time} to {end_time}")

    params = {
        'query': '#FlixBus',
        'start_time': start_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        'end_time': end_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        'tweet_fields': 'id,text,created_at,entities',
        'user_fields': 'id,username,location,public_metrics',
        'next_token': None,
        'expansions': 'author_id,referenced_tweets.id',
        'max_results': 100
    }

    response = search_recent(params)

    if response['meta']['result_count'] > 0:
        tweet_data = response['data']
        if 'users' in response['includes']:
            raw_user = response['includes']['users']
            user_data = {x['id']: x for x in response['includes']['users']}
        if 'tweets' in response['includes']:
            raw_retweet = response['includes']['tweets']
            original_retweet = {x['id']: x for x in response['includes']['tweets']}


        has_next_page = True if 'next_token' in response['meta'] else False

        while has_next_page:
            params['next_token'] = response['meta']['next_token']
            response = search_recent(params)
            tweet_data += response['data']

            if 'users' in response['includes']:
                raw_user += response['includes']['users']
                user_data = {**user_data, **{x['id']: x for x in response['includes']['users']}}

            if 'tweets' in response['includes']:
                raw_retweet += response['includes']['tweets']
                original_retweet = {**original_retweet, **{x['id']: x for x in response['includes']['tweets']}}

            has_next_page = True if 'next_token' in response['meta'] else False

        logger.info("Saving raw data to GCS")
        save_rawdata_gcs(tweet_data+raw_user+raw_retweet, end_time, start_time)
        data = extract_info(tweet_data, user_data, original_retweet)

        client_bq = bigquery.Client.from_service_account_json('credentials/caseflix-5f70b8c20763.json')
        dataset = client_bq.dataset('case_flix')
        table_ref = dataset.table('tweet_data')

        logger.info(f"Deleting from {start_time} to {end_time}")
        delete_data_period(
            table_ref, start_time, end_time, client_bq, disable_override
        )

        job_config = bigquery.LoadJobConfig()

        logger.info("Upload to bq")
        upload_job = client_bq.load_table_from_json(data, table_ref, job_config=job_config)
        upload_job.result()
        logger.info("Job Done")
    else:
        logger.info("No tweets retrieved")


if __name__ == "__main__":
    main()

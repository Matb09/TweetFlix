import requests
from google.cloud import storage
from datetime import datetime
import json
from credentials import BEARER
import time


def search_recent(params):
    """
    retrieve tweets with params - max 7 days back
    """
    query = params['query']
    start_time = params['start_time']
    end_time = params['end_time']
    tweet_fields = params['tweet_fields']
    user_fields = params['user_fields']
    next_token = params['next_token']
    expansions = params['expansions']
    max_results = params['max_results']

    query = {'query': query, 'start_time': start_time, 'end_time': end_time, 'tweet.fields': tweet_fields,
             'next_token': next_token, 'user.fields': user_fields, 'expansions': expansions, 'max_results': max_results}
    headers = {"Authorization": BEARER}

    response = requests.get('https://api.twitter.com/2/tweets/search/recent', params=query, headers=headers)

    if response.status_code == 200:
        data = response.json()
        return data
    elif response.status_code == 429:  # Rate limit, too many request
        time.sleep(60)  # sleep 15min and re-start
        return search_recent(params)
    else:
        raise Exception(response.json()['detail'])


def extract_info(tweet_data, user_data, original_retweet):
    """
    data cleaning and extraction
    """
    cleaned_data = []

    for tweet in tweet_data:
        tmp = dict()
        hashtag = []

        # is retweeted if at least one element of the referenced_tweets has type retweeted
        tmp['retweet'] = True if 'referenced_tweets' in tweet and [True for x in tweet['referenced_tweets'] if x['type'] == 'retweeted'] else False

        # if retweeted i need the full_text, retweet are truncated at 140ch, I need hashtags
        # some tweet may have hashtags originally, but the truncated version doesn't show them
        if tmp['retweet']:
            father_tweet = original_retweet[tweet['referenced_tweets'][0]['id']]

            # override only with complete text and hashtags, other info from retweet
            # tmp['text'] = father_tweet['text']
            for elm in father_tweet['entities']['hashtags']:
                hashtag.append(elm['tag'])

            tmp['hashtag'] = hashtag
        else:
            # tmp['text'] = tweet['text']

            if 'entities' in tweet and 'hashtags' in tweet['entities']:
                for elm in tweet['entities']['hashtags']:
                    hashtag.append(elm['tag'])

                tmp['hashtag'] = hashtag

        tmp['id'] = tweet['id']
        tmp['created_at'] = tweet['created_at']

        # Add user data
        user = user_data[tweet['author_id']]

        tmp['author_id'] = user['id']
        tmp['user_location'] = user['location'] if 'location' in user else None
        tmp['number_of_followers'] = user['public_metrics']['followers_count']
        tmp['number_of_tweet'] = user['public_metrics']['tweet_count']

        cleaned_data.append(tmp)

    return cleaned_data


def save_rawdata_gcs(data, end_time, start_time):
    """
    Save raw_response json to google cloud storage
    """

    bucket_name = 'flix_tweet_raw_data'
    client_gcs = storage.Client.from_service_account_json('credentials/caseflix-5f70b8c20763.json')

    bucket = client_gcs.get_bucket(bucket_name)

    filename = str(start_time) + ' - ' + str(end_time) + ' - ' + str(datetime.now())
    blob = bucket.blob(filename)
    blob.upload_from_string(data=json.dumps(data), content_type='application/json')


def delete_data_period(table_ref, start_time, end_time, client_bq, disable_override=False):
    """
    Deletes data from the table on BQ, if the date column is between start_date and end_date
    """
    delete_query = f"""
        DELETE FROM `{table_ref}`
        WHERE created_at >= CAST('{start_time}' AS TIMESTAMP) AND created_at <= CAST('{end_time}' AS TIMESTAMP)
        """

    if not disable_override:
        delete_query = delete_query.strip()
        query_job = client_bq.query(delete_query)
        query_job.result()
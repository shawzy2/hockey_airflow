from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
import logging

import requests
import json
from datetime import datetime
import boto3

# setting env vars to access AWS resources
# this is not a smart way to store credentials
# but, this is okay for now since we are doing experimental work
s3 = boto3.client('s3',
        aws_access_key_id='xxxx',  # replace with your credentials
        aws_secret_access_key='xxxxx'
    )
    
def get_game_data_from_api(game_id):
    '''Gets event data for specified game
    
    Given the id of a game
    Return a JSON object of all events that happened in game
    '''
    r = requests.get(url='https://statsapi.web.nhl.com/api/v1/game/' + str(game_id) + '/feed/live')
    return r.json()

def get_prev_day_game_data(**context):
    '''Gets event data for all games on previous day
    
    Return list contianing event data for all games that occured on previous day
    '''
    r = requests.get(url='https://statsapi.web.nhl.com/api/v1/schedule?date=' + str(context['yesterday_ds']))
    d = r.json()
    
    # get list of game id's that occur on prev day
    game_ids = []
    if len(d['dates']) >= 1:
        game_ids = [game['gamePk'] for game in d['dates'][0]['games']]
    logging.info('Games on ' + context['yesterday_ds'] + ': ' + str(game_ids))

    # download all game data for these games
    return [get_game_data_from_api(game_id) for game_id in game_ids]

def store_to_s3_bucket(folder, game_id, game_data):
    '''Stores object to S3
    
    Given a folder (i.e. BLOCKED_SHOT), game_id, and game_data
    Store game_data to S3 bucket
    '''
    bucket_name = 'nhl-api-scraper-test'
    s3.put_object(Bucket=bucket_name, 
         Key = folder + '/' + str(game_id) + '.json',
         Body = bytes(json.dumps(game_data).encode('UTF-8'))
    )
    log_message = 'GameID: ' + str(game_id) + '; ' + folder + ': ' + str(len(game_data))
    logging.info(log_message)
    return log_message

def get_data_by_event_type(response, event_type):
    '''Extracts a list of events filtered by event_type
    
    Given a list of events (response) and an event_type
    Return a message describing success/failure of extracting/storing events to S3 bucket
    '''
    for game in response:
        game_id = game['gameData']['game']['pk']
        event_type_list = [play for play in game['liveData']['plays']['allPlays'] if play['result']['eventTypeId'] == event_type]
        try:
            store_to_s3_bucket(event_type, game_id, event_type_list)
        except Exception:
            return 'error when storing ' + event_type
    return 'success'

def get_data_by_sequence(**context):
    '''Extracts a list of events from faceoff to stopage
    
    Given a list of events
    Return a message describing success/failure of extracting/storing events to S3 bucket
    '''
    response = context['ti'].xcom_pull(task_ids='api_scraper')
    for game in response:
        game_id = game['gameData']['game']['pk']
        sequences = []
        current_sequence = []
        in_sequence = False
        sequence_endings = {'STOP', 'GOAL', 'PERIOD_END'}
        
        for play in game['liveData']['plays']['allPlays']:
            event_type = play['result']['eventTypeId'] 
            
            # a sequence starts at a faceoff
            if event_type == 'FACEOFF':
                in_sequence = True
                
            # check if play sequence has ended, add it to list of 'sequences', wait for next faceoff to happen
            if in_sequence and event_type in sequence_endings:
                current_sequence.append(play)
                sequences.append(current_sequence)
                current_sequence = []
                in_sequence = False
            elif in_sequence and event_type == 'FACEOFF' and len(current_sequence) > 1:
                sequences.append(current_sequence)
                current_sequence = []
                current_sequence.append(play)
            elif in_sequence:
                current_sequence.append(play)
            
        try:
            store_to_s3_bucket('SEQUENCE', game_id, sequences)
        except Exception:
            return 'error when storing SEQUENCE'
    return 'success'

def transform_blocked_shot(**context):
    '''Stores JSON list of blocked shots to S3'''
    data = context['ti'].xcom_pull(task_ids='api_scraper')
    return get_data_by_event_type(data, 'BLOCKED_SHOT')

def transform_faceoff(**context):
    '''Stores JSON list of faceoffs to S3'''
    data = context['ti'].xcom_pull(task_ids='api_scraper')
    return get_data_by_event_type(data, 'FACEOFF')

def transform_giveaway(**context):
    '''Stores JSON list of giveaways to S3'''
    data = context['ti'].xcom_pull(task_ids='api_scraper')
    return get_data_by_event_type(data, 'GIVEAWAY')

def transform_goal(**context):
    '''Stores JSON list of goals to S3'''
    data = context['ti'].xcom_pull(task_ids='api_scraper')
    return get_data_by_event_type(data, 'GOAL')

def transform_hit(**context):
    '''Stores JSON list of hits to S3'''
    data = context['ti'].xcom_pull(task_ids='api_scraper')
    return get_data_by_event_type(data, 'HIT')

def transform_penalty(**context):
    '''Stores JSON list of penalties to S3'''
    data = context['ti'].xcom_pull(task_ids='api_scraper')
    return get_data_by_event_type(data, 'PENALTY')

def transform_shot(**context):
    '''Stores JSON list of shots to S3'''
    data = context['ti'].xcom_pull(task_ids='api_scraper')
    return get_data_by_event_type(data, 'SHOT')

def transform_stop(**context):
    '''Stores JSON list of stoppages to S3'''
    data = context['ti'].xcom_pull(task_ids='api_scraper')
    return get_data_by_event_type(data, 'STOP')

def transform_takeaway(**context):
    '''Stores JSON list of takeaway to S3'''
    data = context['ti'].xcom_pull(task_ids='api_scraper')
    return get_data_by_event_type(data, 'TAKEAWAY')


with DAG("nhl_scrape", start_date=datetime(2021, 6, 28), schedule_interval='30 8 * * *', catchup=True) as dag:
    api_scraper = PythonOperator(
            task_id="api_scraper",
            python_callable=get_prev_day_game_data
        )

    process_blocked_shot = PythonOperator(
        task_id="transform_blocked_shot",
        python_callable=transform_blocked_shot
    )

    process_faceoff = PythonOperator(
        task_id="transform_faceoff",
        python_callable=transform_faceoff
    )

    process_giveaway = PythonOperator(
        task_id="transform_giveaway",
        python_callable=transform_giveaway
    )

    process_goal = PythonOperator(
        task_id="transform_goal",
        python_callable=transform_goal
    )

    process_hit = PythonOperator(
        task_id="transform_hit",
        python_callable=transform_hit
    )

    process_penalty = PythonOperator(
        task_id="transform_penalty",
        python_callable=transform_penalty
    )

    process_shot = PythonOperator(
        task_id="transform_shot",
        python_callable=transform_shot
    )

    process_stop = PythonOperator(
        task_id="transform_stop",
        python_callable=transform_stop
    )

    process_takeaway = PythonOperator(
        task_id="transform_takeaway",
        python_callable=transform_takeaway
    )

    process_sequence = PythonOperator(
        task_id="transform_sequence",
        python_callable=get_data_by_sequence
    )

    success = BashOperator(
        task_id="success",
        bash_command="echo 'success'"
    )

    api_scraper >> [process_blocked_shot, process_faceoff, process_giveaway, process_goal, process_hit, process_penalty, process_shot, process_stop, process_takeaway, process_sequence] >> success
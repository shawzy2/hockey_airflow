from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator

import requests
import json
from datetime import datetime, date, timedelta
import boto3


def get_game_data_from_api(game_id):
    r = requests.get(url='https://statsapi.web.nhl.com/api/v1/game/' + str(game_id) + '/feed/live')
    return r.json()

def get_prev_day_game_data():
    date_lookup = date.today() - timedelta(days=1)
    r = requests.get(url='https://statsapi.web.nhl.com/api/v1/schedule?date=' + str(date_lookup))
    d = r.json()
    
    # get list of game id's that occur on prev day
    game_ids = []
    if len(d['dates']) >= 1:
        game_ids = [game['gamePk'] for game in d['dates'][0]['games']]
    print(game_ids)

    # download all game data for these games
    game_data = [get_game_data_from_api(game_id) for game_id in game_ids]

    return game_data

s3 = boto3.client('s3',
        aws_access_key_id='AKIA3XQPCT7XQSCEFIIC',
        aws_secret_access_key='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'
    )

def store_to_s3_bucket(folder, game_id, game_data):
    bucket_name = 'nhl-api-scraper-test'
    s3.put_object(Bucket=bucket_name, 
         Key = folder + '/' + str(game_id) + '.json',
         Body = bytes(json.dumps(game_data).encode('UTF-8'))
    )

def get_data_by_event_type(response, event_type):
    for game in response:
        game_id = game['gameData']['game']['pk']
        event_type_list = [play for play in game['liveData']['plays']['allPlays'] if play['result']['eventTypeId'] == event_type]
        try:
            store_to_s3_bucket(event_type, game_id, event_type_list)
        except Exception:
            return 'error when storing ' + event_type
    return 'success'

def get_data_by_sequence(response):
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

def process_prev_day_game_data(ti):
    response = ti.xcom_pull(task_ids='api_scraper')
    event_types = ['BLOCKED_SHOT', 'FACEOFF', 'GIVEAWAY', 'GOAL', 'HIT', 'MISSED_SHOT', 
                       'PENALTY', 'SHOT', 'STOP', 'TAKEAWAY']
    try:
        for event_type in event_types:
            get_data_by_event_type(response, event_type)
    except Exception:
        return 'failure'
    return 'success'


with DAG("api_scraper", start_date=datetime(2021, 6, 2), schedule_interval='30 4 * * *', catchup=True) as dag:
    api_scraper = PythonOperator(
            task_id="api_scraper",
            python_callable=get_prev_day_game_data
        )

    data_processor_loader = BranchPythonOperator(
        task_id="data_processor_loader",
        python_callable=process_prev_day_game_data
    )

    success = BashOperator(
        task_id="success",
        bash_command="echo 'success'"
    )

    failure = BashOperator(
        task_id="failure",
        bash_command="echo 'failure'"
    )

    api_scraper >> data_processor_loader >> [success, failure]
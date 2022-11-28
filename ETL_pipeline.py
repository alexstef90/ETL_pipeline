from datetime import datetime, timedelta
import pandas as pd
import pandahouse as ph
from io import StringIO
import requests
import requests
import numpy as np

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

# Функция загрузуки данных из БД в датафрейм

def select(query):
    connection = {'host': 'https://clickhouse.lab.karpov.courses',
                  'database':'simulator_20220920',
                  'user':'student', 
                  'password':'dpo_python_2020'}
    data_frame = ph.read_clickhouse(query, connection=connection)
    return data_frame

# Дефолтные параметры для тасков
default_args = {
                'owner': 'a-stefanovich',
                'depends_on_past': False,
                'retries': 2,
                'retry_delay': timedelta(minutes=5),
                'start_date': datetime(2022, 10, 16),
               }

# Интервал запуска DAG
schedule_interval = '0 11 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_avstefanovich():
    
    @task
    def extract_mes():

        query = '''select
        event_date, users, gender, age, os, messages_sent, messages_received, users_sent, users_received
        from
        (select toDate(time) as event_date, user_id as users, uniq(reciever_id) as users_sent, count() as messages_sent, gender, age, os
        from simulator_20220920.message_actions
        where toDate(time) = yesterday()
        group by event_date, users, gender, os, age) t1 

        full outer join
        (select toDate(time) as event_date, gender, os, age, reciever_id as users, count() as messages_received, uniq(user_id) as users_received
        from simulator_20220920.message_actions
        where toDate(time) = yesterday()
        group by event_date, users, gender, age, os)t2 
        using users'''

        df_cube1 = select(query = query)
        return df_cube1
    
    @task
    def extract_feed():
        query1 = '''select user_id as users, toDate(time) as event_date, gender, age, os, countIf(action='view') as views, countIf(action='like') as likes
        from simulator_20220920.feed_actions
        where toDate(time) = yesterday()
        group by event_date, gender, age, os, users'''

        df_cube2 = select(query1)
        return df_cube2
    
    @task
    def transform(df_cube1, df_cube2):
        df_cube = df_cube1.merge(df_cube2, how='outer', on=['users','event_date', 'gender','age','os']).dropna()
        return df_cube
    
    @task
    def os(df_cube):
        df_os = df_cube[['event_date', 'os', 'views', 'likes', 'messages_received', 'messages_sent', 'users_received', 'users_sent']]\
                .groupby(['event_date', 'os'], dropna = False, as_index = False)\
                .sum()
        df_os.rename(columns={'os': 'dimension_value'}, inplace = True)
        df_os.insert(1, 'dimension', 'os')
        return df_os
    
    @task
    def gender(df_cube):
        df_gender = df_cube[['event_date', 'gender', 'views', 'likes', 'messages_received', 'messages_sent', 'users_received', 'users_sent']]\
                .groupby(['event_date', 'gender'], dropna = False, as_index = False)\
                .sum()
        df_gender.rename(columns={'gender': 'dimension_value'}, inplace = True)
        df_gender.insert(1, 'dimension', 'gender')
        return df_gender
    
    @task
    def age(df_cube):
        df_age = df_cube[['event_date', 'age', 'views', 'likes', 'messages_received', 'messages_sent', 'users_received', 'users_sent']]\
                .groupby(['event_date', 'age'], dropna = False, as_index = False)\
                .sum()
        df_age.rename(columns={'age': 'dimension_value'}, inplace = True)
        df_age.insert(1, 'dimension', 'age')
        return df_age
    
    
    @task
    def concat(df_os, df_gender, df_age):
        
        df_total = pd.concat([df_os, df_gender, df_age],ignore_index=False).reset_index(drop=True)

        return df_total

    @task
    def load(df_total):

        connect = {
        'host': 'https://clickhouse.lab.karpov.courses',
        'password': '656e2b0c9c',
        'user': 'student-rw',
        'database': 'test'}

        query_new_table = '''CREATE TABLE IF NOT EXISTS test.stefanov
                            (event_date DATE,
                            dimension String,
                            dimension_value String,
                            views Float64,
                            likes Float64,
                            messages_received Float64,
                            messages_sent Float64,
                            users_received Float64,
                            users_sent Float64)
                            ENGINE = MergeTree()
                            ORDER BY (event_date);
                            '''
       
        ph.execute(connection=connect, query = query_new_table)

        ph.to_clickhouse(df_total, 'stefanov', index=False, connection = connect)

        
    df_cube1 = extract_mes()
    
    df_cube2 = extract_feed()
    
    df_cube = transform(df_cube1, df_cube2)
    
    df_os = os(df_cube)
    
    df_gender = gender(df_cube)
    
    df_age = age(df_cube)

    df_total = concat(df_os, df_gender, df_age)
   
    load(df_total)
    
dag_avstefanovich = dag_avstefanovich()    
    
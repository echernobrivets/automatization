import pandahouse as ph
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from datetime import datetime, timedelta

import io
import telegram

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

connection = {'host': 'https://clickhouse.lab.karpov.courses',
                      'database':'simulator_20221220',
                      'user':'student', 
                      'password':'dpo_python_2020'
                     }


default_args = {
    'owner': 'e-chernobrivets',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 30)
}


schedule_interval = '0 11 * * *'

my_token = '5482453777:AAEqFGVojT-mF8i--T2VRPGo_1hDdaDyn9M'
bot = telegram.Bot(token=my_token)
chat_id = -850804180

# DAG
@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def feed_and_message_dag_chernobrivets_7_2():
      
    @task
    def extract_feed_actions():
        q_1 = """
            SELECT 
                toDate(time) AS day,
                uniqExact(user_id) AS users,
                countIf(action='like') AS likes,
                countIf(action='view') AS views,
                likes / users AS likes_per_user,
                views / users AS views_per_user,
                likes / views AS ctr
            FROM {db}.feed_actions
            WHERE toDate(time) between today() - 8 AND yesterday()
            GROUP BY day
            """
        df_feed = ph.read_clickhouse(q_1, connection=connection)
        return df_feed
    
    @task
    def extract_message_actions():
        q_2 = """
            SELECT 
                toDate(time) AS day,
                uniqExact(user_id) AS sender_users,
                count(user_id) AS messages,
                messages / sender_users AS messages_per_user
            FROM {db}.message_actions
            WHERE toDate(time) between today() - 8 AND yesterday()
            GROUP BY day
            """

        df_message = ph.read_clickhouse(q_2, connection=connection)
        return df_message
    
    @task
    def feed_and_message_merge(df_feed, df_message):
        merge_tables = df_feed.merge(df_message, on='day')
        return merge_tables
    
    @task()
    def send_message_yesterday(merge_tables, chat_id):
        day = merge_tables['day'].loc[0]
        dau_by_feed = merge_tables['users'].loc[0]
        dau_by_message = merge_tables['sender_users'].loc[0]
        likes = merge_tables['likes'].loc[0]
        views = merge_tables['views'].loc[0]
        ctr = round(merge_tables['ctr'].loc[0], 2)
        count_messages = merge_tables['messages'].loc[0]
        
        message = f"""
                    Добрый день!
                    Ключевые метрики ленты новостей и мессенджера:
                    дата: {day},
                    DAU ленты новостей: {dau_by_feed},
                    DAU мессенджера: {dau_by_message},
                    CTR ленты новостей: {ctr},
                    Лайки: {likes},
                    Просмотры: {views},    
                    Отправленно сообщений: {count_messages}    
                """
        bot.sendMessage(chat_id=chat_id, text=message)
        
    @task()
    def send_photo_last_week(merge_tables, chat_id):
        fig, ax = plt.subplots(2, 2, figsize=(20, 14))
        fig.suptitle('Динамика показателей за последние 7 дней', fontsize=30)

        sns.lineplot(ax=ax[0,0], data = merge_tables, x='day', y='users', label = 'DAU ленты новостей', linewidth = 3)
        sns.lineplot(ax=ax[0,0], data = merge_tables, x='day', y='sender_users', label = 'DAU мессенджера', linewidth = 3)
        ax[0,0].set_title('DAU ленты новостей и мессенджера')
        ax[0,0].legend()
        ax[0,0].grid()

        sns.lineplot(ax=ax[0,1], data = merge_tables, x='day', y='ctr', linewidth = 3)
        ax[0,1].set_title('CTR ленты новостей')
        ax[0,1].grid()

        sns.lineplot(ax=ax[1,0], data = merge_tables, x='day', y='likes', label = 'Лайки', linewidth = 3)
        sns.lineplot(ax=ax[1,0], data = merge_tables, x='day', y='views', label = 'Просмотры', linewidth = 3)
        sns.lineplot(ax=ax[1,0], data = merge_tables, x='day', y='messages', label = 'Сообщения', linewidth = 3)
        ax[1,0].set_title('Все события')
        ax[1,0].legend()
        ax[1,0].grid()

        sns.lineplot(ax = ax[1,1], data = merge_tables, x = 'day', y = 'likes_per_user', label = 'Лайки', linewidth = 3)
        sns.lineplot(ax = ax[1,1], data = merge_tables, x = 'day', y = 'views_per_user', label = 'Просмотры', linewidth = 3)
        sns.lineplot(ax = ax[1,1], data = merge_tables, x = 'day', y = 'messages_per_user', label = 'Сообщения', linewidth = 3)
        ax[1,1].set_title('События на 1 пользователя')
        ax[1,1].grid()
        
        plt.tight_layout()

        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'chernobrivets_statistic_7_2.png'
        plt.close()
        
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)
        
   
    
    
    df_feed = extract_feed_actions()
    df_message = extract_message_actions()
    merge_tables = feed_and_message_merge(df_feed, df_message)
    send_message_yesterday(merge_tables, chat_id)
    send_photo_last_week(merge_tables, chat_id)

feed_and_message_dag_chernobrivets_7_2 = feed_and_message_dag_chernobrivets_7_2()

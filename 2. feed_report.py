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

connection = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'password': 'dpo_python_2020',
    'user': 'student',
    'database': 'simulator_20221220'
}

default_args = {
    'owner': 'e-chernobrivets',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 29),
}

schedule_interval = '0 11 * * *'

my_token = '5482453777:AAEqFGVojT-mF8i--T2VRPGo_1hDdaDyn9M'
bot = telegram.Bot(token=my_token)

chat_id = '-850804180'    
    
@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def feed_report_chernobrivets_lesson_7_1():
    
    @task
    def yesterday_extract():
        q_1 = """
            SELECT
                max(toDate(time)) as day, 
                count(DISTINCT user_id) as DAU, 
                sum(action = 'like') as likes,
                sum(action = 'view') as views, 
                likes/views as CTR
            FROM simulator_20221220.feed_actions
            WHERE toDate(time) = yesterday()
            """

        yesterday_report = ph.read_clickhouse(q_1, connection=connection)
        return yesterday_report

    @task
    def last_week_extract():
    
        q_2 = """
            SELECT
                toDate(time) as day, 
                count(DISTINCT user_id) as DAU, 
                sum(action = 'like') as likes,
                sum(action = 'view') as views, 
                likes/views as CTR
            FROM simulator_20221220.feed_actions
            WHERE toDate(time) between today() - 8 AND yesterday()
            GROUP BY day
            """
        weekly_report = ph.read_clickhouse(q_2, connection=connection)
        return weekly_report
    
    @task()
    def send_message_by_yesterday(yesterday_report, chat_id):
        day = yesterday_report['day'].loc[0].strftime('%Y-%m-%d')
        dau = yesterday_report['DAU'].loc[0]
        views = yesterday_report['views'].loc[0]
        likes = yesterday_report['likes'].loc[0]
        ctr = yesterday_report['CTR'].loc[0]
                
        message = f"""
                    Добрый день!
                    Ключевые метрики ленты новостей:
                    Дата: {day},
                    DAU: {dau},
                    CTR: {ctr},
                    Лайки: {likes},
                    Просмотры: {views}   
                """
        bot.sendMessage(chat_id=chat_id, text=message)
    
    @task
    def send_pic_metrics_by_week(weekly_report, chat_id):
        fig, axes = plt.subplots(2, 2, figsize=(20, 14))

        fig.suptitle('Динамика показателей за последние 7 дней', fontsize=30)

        sns.lineplot(ax = axes[0, 0], data = weekly_report, x = 'day', y = 'DAU')
        axes[0, 0].set_title('DAU')
        axes[0, 0].grid()

        sns.lineplot(ax = axes[0, 1], data = weekly_report, x = 'day', y = 'CTR')
        axes[0, 1].set_title('CTR')
        axes[0, 1].grid()

        sns.lineplot(ax = axes[1, 0], data = weekly_report, x = 'day', y = 'views')
        axes[1, 0].set_title('Просмотры')
        axes[1, 0].grid()

        sns.lineplot(ax = axes[1, 1], data = weekly_report, x = 'day', y = 'likes')
        axes[1, 1].set_title('Лайки')
        axes[1, 1].grid()

        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'Statistic_lesson_7_1.png'
        plt.close()
    
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)
        
    yesterday_report = yesterday_extract()
    weekly_report = last_week_extract()
    send_message_by_yesterday(yesterday_report, chat_id)
    send_pic_metrics_by_week(weekly_report, chat_id)
        

feed_report_chernobrivets_lesson_7_1 = feed_report_chernobrivets_lesson_7_1()
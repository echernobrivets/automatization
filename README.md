# 1. etl_pipeline.
### Параллельно обрабатывал 2 таблицы. Из первой таблицы для каждого пользователя посчитали число просмотров и лайков контента. Из второй - для каждого пользователя считали, сколько он получает и отсылает сообщений, скольким людям он пишет, сколько людей пишут ему. Каждая выгрузка была в отдельном таске.
### Далее объединили две таблицы в одну. Для этой таблицы считали все эти метрики в разрезе по полу, возрасту и ос. Делали три разных таска на каждый срез. Финальные данные со всеми метриками записали в отдельную таблицу в ClickHouse таким образом, чтобы каждый день таблица дополнялась новыми данными.
### На выходе получился DAG в airflow, который считает данные каждый день за вчера.
# 2. feed_report.
### Был создан телеграм-бот, который делал автоматическую отправку аналитической сводки в телеграм каждое утро в 11:00. Отчет состоит из 2-х частей:
 - текст с информацией о значениях ключевых метрик за предыдущий день;
 - график со значениями ключевых метрик (DAU, likes, views, CTR) за предыдущие 7 дней.
# 3. app_report.
### Собран единый отчет по работе всего приложения. Задачей было продумать, какие метрики необходимо отобразить в отчете? Как можно показать их динамику? Приложить к отчету графики или файлы, чтобы сделать его более наглядным и информативным.

# 1. etl_pipeline.
### Параллельно обрабатывал 2 таблицы. Из первой таблицы для каждого пользователя посчитали число просмотров и лайков контента. Из второй - для каждого пользователя считали, сколько он получает и отсылает сообщений, скольким людям он пишет, сколько людей пишут ему. Каждая выгрузка была в отдельном таске. Далее объединяем две таблицы в одну. Для этой таблицы считаем все эти метрики в разрезе по полу, возрасту и ос. Делаем три разных таска на каждый срез. Финальные данные со всеми метриками записываем в отдельную таблицу в ClickHouse таким образом, чтобы каждый день таблица дополнялась новыми данными.
### На выходе вышел DAG в airflow, который считает данные каждый день за вчера.
#
###

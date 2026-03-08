Итоговое домашнее задание по дисциплине ETL-процессы предполагает создание системы репликации данных из MongoDB в PostgreSQL с использованием Apache Airflow и построение аналитических витрин.

Настройка проекта:
- Запуск MongoDB в docker: 
```
docker run -d \
  --name mongodb \
  -p 27017:27017 \
  -e MONGO_INITDB_ROOT_USERNAME=admin \
  -e MONGO_INITDB_ROOT_PASSWORD=pass \
  mongo:latest
```
- Запуск PostgreSQL в docker:
```
docker run -d \
  --name postgres \
  -p 5432:5432 \
  -e POSTGRES_USER=airflow \
  -e POSTGRES_PASSWORD=airflow \
  -e POSTGRES_DB=analytics \
  postgres:15
```

- Создание виртуального окружения python и настройка зависимостей
```
python3 -m venv .venv
source .venv/bin/activate   
pip install -r requirements.txt
```
- generate_fake_data.py - Генерация синтетических данных в MongoDB из скрипта
- scripts/init.sql - Создание таблиц в PostgreSQL
```
python3 generate_fake_data.py
psql -d analytics -U airflow -f init.sql
```
- Запуск контейнера Airflow
```
docker-compose up -d
```

В UI Airflow предусмотрено два DAG файла:
- data_replication_dag реплицирует источник данных (mongodb) в Postgres
- mart_creation_dag создает витрины данных 


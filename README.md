# ChicagoCrashesETL

jak teraz uruchomić:
1. przenieść data do folderu ```airflow-postgres-docker```
2. zbudowac od jeszcze raz kontener bo były zmiany w Dockerfile i docker-compose: ```docker-compose up -d --build```
3. uruchomić kontener: ```docker exec -it airflow-postgres-docker-airflow-webserver-1 bash ```
4. przejść do ```opt/airflow```
5. uruchomić extract: ```python -m etl.extract.extract```
6. uruchumić transform: ```python -m etl.transform.transform_all```
7. uruchomić load dim_date: ```python -m etl.load.load_dim_date``


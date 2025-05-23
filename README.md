# ChicagoCrashesETL

jak teraz uruchomić:
1. zbudować kontener (bo zmiany w Dockerfile) z folderu ```airflow-postgres-docker```: komenda ```docker build -t airflow-postgres-docker . ```
2. uruchomic kontener: ```docker-compose up -d```
3. extract: ```python -m etl.extract.extract```
4. transform: ```python -m etl.transform.transform_all```

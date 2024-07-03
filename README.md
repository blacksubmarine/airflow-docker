- 1 run these commands to setup the image
'''
mkdir ./dags ./logs ./plugins


docker compose up airflow-init

docker-compose up -d --no-deps --build airflow-webserver airflow-scheduler

'''

- 2 Open browser and type http://localhost:8080 to launch the airflow webserver


-3 To extend the image use:

```
docker build . --tag extending_airflow:latest
```
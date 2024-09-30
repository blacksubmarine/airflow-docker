- 1 run these commands to setup the image

Check in repo if we have these 3 folders if not create them

```
mkdir ./dags ./logs ./plugins
```

- 2 setup the container with:

```
docker build -t extending_airflow .
```

- 2 Open browser and type http://localhost:8080 to launch the airflow webserver


-3 To extend the image use:

```
docker build . --tag extending_airflow:latest
```

-4 Run the image with:

```
docker compose up
```

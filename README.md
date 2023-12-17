# Data Flow 예제

## Run
### Build image
```
docker build -t dataflow-example ./     
```

### Setup .env
```
echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env
```

### Run docker-compose
```
docker-compose up airflow-init
docker-compose up
```
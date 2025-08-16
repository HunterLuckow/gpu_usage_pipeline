# GPU Usage Monitoring & Billing Pipeline


# Setting Up Airflow
# Documentation https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html

Web UI: http://localhost:8080

1. Initialize DB (first time only)
docker compose up airflow-init

2. Start all services
docker compose up

# Gracefully shut down services
ctrl + c

3. If everything is healthy, stop with Ctrl+C and run in the background:
# They recommend docker compose up (no -d) for first-time runs so you can watch the containers 
# initialize and check for errors.
# Once everything is healthy, you can stop (Ctrl+C) and then run docker compose up -d for routine 
# use.
docker compose up -d

4. Check container health
docker ps

### If making a change to docker-compose.yaml ###
Once you make this change, restart your Airflow stack:
docker compose down
docker compose run airflow-init
docker compose up -d

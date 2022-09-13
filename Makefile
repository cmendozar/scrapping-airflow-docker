build:
	docker build . --tag extending_airflow:latest

run:
	docker compose up -d

disarm:
	docker compose down -v

healthy-check:
	docker container ls


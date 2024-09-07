
up:
	docker compose up -d

down:
	docker compose down --volumes --remove-orphans

test:
	docker compose run --rm airflow-tests


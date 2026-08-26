# =============================================================================
# Mobility Analytics Lakehouse Makefile
# Convenience automation targets for developers and data engineers
# =============================================================================

.PHONY: help up down status produce bronze silver gold all-jobs dashboard api maintenance test lint clean

help:
	@echo "Mobility Analytics Lakehouse CLI Helper"
	@echo "----------------------------------------"
	@echo "make up          - Start all Docker infrastructure containers"
	@echo "make down        - Tear down Docker containers and prune network"
	@echo "make status      - Display status of Docker services"
	@echo "make produce     - Run the smart city IoT traffic event generator"
	@echo "make bronze      - Submit Bronze streaming ingestion pipeline"
	@echo "make silver      - Submit Silver clean & DLQ quarantine pipeline"
	@echo "make gold        - Submit Gold star schema dimensional pipeline"
	@echo "make all-jobs    - Submit Bronze, Silver, and Gold streaming jobs"
	@echo "make maintenance - Run Delta Lake OPTIMIZE, Z-ORDER & VACUUM job"
	@echo "make dashboard   - Launch Streamlit analytics, 3D map & AI dashboard"
	@echo "make api         - Launch FastAPI REST serving microservice"
	@echo "make test        - Run unit test suite with 21 test cases"
	@echo "make lint        - Run Flake8 code linting checks"
	@echo "make clean       - Remove cached Spark logs, IVY files, and bytecode"

up:
	docker compose up -d
	@echo "Waiting 10s for Kafka broker..."
	@sleep 10
	docker exec -i kafka /opt/kafka/bin/kafka-topics.sh --create --if-not-exists --topic traffic-topic --bootstrap-server kafka:9092 --partitions 3 --replication-factor 1

down:
	docker compose down

status:
	docker compose ps

produce:
	python producer/traffic_producer.py --rate 3.0 --dirty-ratio 0.25

bronze:
	bash scripts/submit_streaming_jobs.sh bronze

silver:
	bash scripts/submit_streaming_jobs.sh silver

gold:
	bash scripts/submit_streaming_jobs.sh gold

all-jobs:
	bash scripts/submit_streaming_jobs.sh all

maintenance:
	docker exec -it spark-worker /opt/spark/bin/spark-submit \
	  --conf spark.jars.ivy=/tmp/.ivy \
	  --packages io.delta:delta-spark_2.12:3.2.0 \
	  /opt/spark-apps/maintenance.py

dashboard:
	streamlit run dashboard/app.py

api:
	uvicorn api.main:app --host 0.0.0.0 --port 8000 --reload

test:
	python -m unittest discover -s tests -p "test_*.py" -v

lint:
	flake8 . --max-line-length=127 --exclude=.git,__pycache__,warehouse,spark-ivy

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	rm -rf .pytest_cache .coverage htmlcov

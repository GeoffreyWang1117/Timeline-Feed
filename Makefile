.PHONY: help install dev build start test docker-up docker-down seed clean

help:
	@echo "Timeline Feed - Makefile Commands"
	@echo ""
	@echo "  make install      - Install dependencies"
	@echo "  make dev          - Run in development mode"
	@echo "  make build        - Build TypeScript"
	@echo "  make start        - Start production server"
	@echo "  make test         - Run tests"
	@echo "  make docker-up    - Start Docker services"
	@echo "  make docker-down  - Stop Docker services"
	@echo "  make seed         - Generate test data"
	@echo "  make clean        - Clean build files"

install:
	npm install

dev:
	npm run dev

build:
	npm run build

start:
	npm start

test:
	npm test

docker-up:
	docker-compose up -d
	@echo "Waiting for services to be healthy..."
	@sleep 10
	@docker-compose ps

docker-down:
	docker-compose down

seed:
	npm run seed follow-graph 100 20

clean:
	rm -rf dist node_modules coverage logs

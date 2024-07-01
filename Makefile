up:
	docker-compose up -d

test: up
	cargo test

down:
	docker-compose down --volumes

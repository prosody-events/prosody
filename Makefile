bootstrap:
	curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
	cargo install cargo-udeps cargo-tarpaulin cargo install taplo-cli bacon

up:
	docker-compose up -d

update:
	cargo update

console:
	open "http://localhost:8080/topics"

format:
	cargo fmt
	taplo fmt

build:
	cargo build

check:
	cargo check

check-watch:
	bacon

quality:
	cargo clippy

quality-watch:
	bacon --job clippy

test: up
	cargo test

test-watch: up
	bacon test

coverage: up
	cargo tarpaulin

dependencies:
	cargo +nightly udeps

reset:
	docker-compose down --volumes

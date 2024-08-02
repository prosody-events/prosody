# Install Rust and necessary tools
bootstrap:
	curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
	cargo install cargo-udeps cargo-tarpaulin taplo-cli bacon

# Start Kafka and related services using Docker Compose
up:
	docker-compose up -d

# Update project dependencies
update:
	cargo update

# Open the Kafka console in a web browser
console:
	open "http://localhost:8080/topics"

# Format Rust code and TOML files
format:
	cargo fmt
	taplo fmt

# Build the project
build:
	cargo build

# Check for compilation errors without building
check:
	cargo check

# Watch for changes and check for compilation errors
check-watch:
	bacon

# Run Clippy for linting
lint:
	cargo clippy

# Watch for changes and run Clippy
lint-watch:
	bacon --job clippy

# Run tests (starts Kafka services first)
test: up
	cargo test

# Watch for changes and run tests
test-watch: up
	INTEGRATION_TESTS=1 bacon test

# Generate code coverage report
coverage: up
	cargo tarpaulin

# Check for unused dependencies
# note: requires installing nightly with `rustup install nightly`
dependencies:
	cargo +nightly udeps

# Stop and remove Docker containers and volumes
reset:
	docker-compose down --volumes

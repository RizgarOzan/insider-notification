# Tests need a Postgres. `make test` starts one in docker; or set TEST_DATABASE_URL yourself.
TEST_DB ?= postgres://notif:notif@localhost:5433/notif_test?sslmode=disable

.PHONY: up test test-db lint run
up:
	docker compose up --build

test-db:
	-docker run -d --rm --name notif-test-db -e POSTGRES_USER=notif -e POSTGRES_PASSWORD=notif \
		-e POSTGRES_DB=notif_test -p 5433:5432 postgres:16-alpine
	@until docker exec notif-test-db pg_isready -U notif -d notif_test >/dev/null 2>&1; do sleep 1; done

test: test-db
	REQUIRE_DB=1 TEST_DATABASE_URL="$(TEST_DB)" go test -race -count=1 ./...

lint:
	go vet ./...
	test -z "$$(gofmt -l .)"

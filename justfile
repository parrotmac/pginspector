
db:
    #!/usr/bin/env bash

    docker compose down || true
    docker compose up -d

    did_start=false
    for i in {1..10}; do
        if docker compose exec db pg_isready; then
            did_start=true
            break
        fi
        sleep 1
    done
    if ! $did_start; then
        echo "Failed to start database"
        exit 1
    fi

    psql "postgres://postgres:postgres@localhost:54322/postgres" -f init.sql

gen: db
    pggen gen go \
      --postgres-connection "postgres://postgres:postgres@localhost:54322/postgres" \
      --query-glob models/**/queries*.sql \
      --go-type 'name=string' --go-type 'varchar=*string' --go-type 'int8=int' --go-type 'text=*string' --go-type 'uuid=github.com/google/uuid.UUID' --go-type 'timestamp=*time.Time' --go-type 'timestampz=*time.Time' --go-type 'jsonb=[]byte'
    mkdir -p generated_models
    go run . -output generated_models/generated_queries.sql
    pggen gen go \
      --postgres-connection "postgres://postgres:postgres@localhost:54322/postgres" \
      --query-glob generated_models/**/generated_queries.sql \
      --go-type 'name=string' --go-type 'varchar=*string' --go-type 'int8=int' --go-type 'text=*string' --go-type 'uuid=github.com/google/uuid.UUID' --go-type 'timestamp=*time.Time' --go-type 'timestampz=*time.Time' --go-type 'jsonb=[]byte'
    goimports -w models/*.sql.go
    goimports -w generated_models/*.sql.go

package main

import (
	"bytes"
	"context"
	"fmt"
	"github.com/jackc/pgx/v4/pgxpool"
	"log"
	"os"
	"strings"
	"testing"
)

func red(s string) string {
	return fmt.Sprintf("\033[31m%s\033[0m", s)
}

func green(s string) string {
	return fmt.Sprintf("\033[32m%s\033[0m", s)
}

func temporaryDatabase(ctx context.Context, initSQL string) string {
	connectionString := os.Getenv("PGINSPECTOR_TEST_DATABASE_URL")
	if connectionString == "" {
		connectionString = "postgres://postgres:postgres@localhost:5432/postgres"
	}

	connStringURL, err := pgxpool.ParseConfig(connectionString)
	if err != nil {
		log.Fatal(err)
	}

	db, err := pgxpool.Connect(ctx, connectionString)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	_, err = db.Exec(context.Background(), "DROP DATABASE IF EXISTS pginspector_test;")
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec(context.Background(), "CREATE DATABASE pginspector_test;")
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec(context.Background(), "GRANT ALL PRIVILEGES ON DATABASE pginspector_test TO postgres;")
	if err != nil {
		log.Fatal(err)
	}

	newDatabaseConnectionString := fmt.Sprintf("postgres://%s:%s@%s:%d/pginspector_test",
		connStringURL.ConnConfig.User,
		connStringURL.ConnConfig.Password,
		connStringURL.ConnConfig.Host,
		connStringURL.ConnConfig.Port,
	)

	db, err = pgxpool.Connect(ctx, newDatabaseConnectionString)
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec(context.Background(), initSQL)
	if err != nil {
		log.Fatal(err)
	}

	return newDatabaseConnectionString
}

func TestConfiguration(t *testing.T) {
	exampleAsReader := strings.NewReader(exampleConfig)
	configuration, err := ReadConfig(exampleAsReader)
	if err != nil {
		t.Fatal(err)
	}

	if len(configuration.SchemaConfig) != 1 {
		t.Fatalf("expected 1 schema config, got %d", len(configuration.SchemaConfig))
	}

	publicSchemaConfig, ok := configuration.SchemaConfig["public"]
	if !ok {
		t.Fatal("expected public schema config")
	}

	if publicSchemaConfig.DefaultPrimaryKeyColumn != "id" {
		t.Fatalf("expected default primary key column to be id, got %s", publicSchemaConfig.DefaultPrimaryKeyColumn)
	}

	if len(publicSchemaConfig.SkipTables) != 1 {
		t.Fatalf("expected 1 skipped table, got %d", len(publicSchemaConfig.SkipTables))
	}
}

func TestGenerateSimpleCase(t *testing.T) {
	const cfgFile = `schema_config:
  public:
    default_primary_key_name: id
    skip_tables:
      - migrations
    table_config:
      person:
`
	exampleAsReader := strings.NewReader(cfgFile)
	configuration, err := ReadConfig(exampleAsReader)
	if err != nil {
		t.Fatal(err)
	}

	connectionString := temporaryDatabase(context.TODO(), `
CREATE TABLE public.person (
	id SERIAL PRIMARY KEY,
	name TEXT NOT NULL
);
`)

	outputBuf := &bytes.Buffer{}

	err = generate(context.TODO(), connectionString, configuration, outputBuf)
	if err != nil {
		t.Fatal(err)
	}

	expectedOutput := `-- File generated by pginspector. DO NOT EDIT.



-- name: SelectPersonByID :one
SELECT
        id,
        name
FROM public.person
WHERE id = pggen.arg('id');

-- name: SelectPersonList :many
SELECT
        id,
        name
FROM public.person;

-- name: UpdatePerson :one
UPDATE public.person
SET (
        id,
        name
) = (
        pggen.arg('id'),
        pggen.arg('name')
) WHERE id = pggen.arg('id') RETURNING *;`

	if outputBuf.String() != expectedOutput {
		t.Fatalf("expected output to be:\n%s\nbut got:\n%s", green(expectedOutput), red(outputBuf.String()))
	}
}

package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"github.com/jackc/pgx/v4"
	"io"
	"log"
	"os"
	"os/signal"
	"sort"
	"syscall"
	"text/template"

	"github.com/iancoleman/strcase"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/parrotmac/pginspector/models"
	"github.com/pkg/errors"
	"gopkg.in/yaml.v3"
)

type TableConfig struct {
	ProtoName               string `yaml:"proto_name"`
	PrimaryKey              string `yaml:"primary_key"`
	GenerateFieldMaskUpdate bool   `yaml:"generate_field_mask_update"`
}

type SchemaConfig struct {
	TableConfig             map[string]TableConfig `yaml:"table_config"`
	DefaultPrimaryKeyColumn string                 `yaml:"default_primary_key_name"`
	SkipTables              []string               `yaml:"skip_tables"`
}

func (s *SchemaConfig) ShouldSkipTable(tableName string) bool {
	for _, t := range s.SkipTables {
		if t == tableName {
			return true
		}
	}
	return false
}

func (s *SchemaConfig) GetTableConfig(tableName string) TableConfig {
	if s.TableConfig != nil {
		if cfg, ok := s.TableConfig[tableName]; ok {
			return cfg
		}
	}
	return TableConfig{
		ProtoName:               "",
		PrimaryKey:              s.DefaultPrimaryKeyColumn,
		GenerateFieldMaskUpdate: false,
	}
}

type GeneratorConfiguration struct {
	SchemaConfig map[string]SchemaConfig `yaml:"schema_config"`
}

const exampleConfig = `
schema_config:
  public:
    default_primary_key_name: id
    skip_tables:
      - migrations
    table_config:
      person:
        proto_name: v1.Person
        generate_field_mask_update: true
      vehicle:
        proto_name: v1.Vehicle
        generate_field_mask_update: true
      model:
        proto_name: v1.Model
        primary_key: id
      manufacturer:
        proto_name: v1.Manufacturer
      rental:
        proto_name: v1.Rental
        generate_field_mask_update: true
      ownership:
        proto_name: v1.Ownership
        generate_field_mask_update: true
`

func ReadConfig(reader io.Reader) (GeneratorConfiguration, error) {
	cfg := GeneratorConfiguration{}
	err := yaml.NewDecoder(reader).Decode(&cfg)
	if err != nil {
		return cfg, errors.WithMessage(err, "Unable to parse config file")
	}
	return cfg, nil
}

type Relation struct {
	Forward bool
	Table   *Table
	Column  *Column
}

type Column struct {
	Name     string
	PGType   string
	Nullable bool
	Default  string
	Relation Relation
}

type Table struct {
	Schema  string
	Name    string
	Columns []Column
}

type GenerationTable struct {
	Table
	Config TableConfig
}

func (t *Table) PrettyPrint() {
	fmt.Printf("Table: %s.%s\n", t.Schema, t.Name)
	for _, c := range t.Columns {
		fmt.Printf("\t%s: %s (default=%s) (nullable=%t) (relation:=%+v)\n", c.Name, c.PGType, c.Default, c.Nullable, c.Relation)
	}
}

type Schema struct {
	Tables map[string]Table
}

func Unwrap[T any](p *T) T {
	if p == nil {
		v := new(T)
		return *v
	}
	return *p
}

func (s *Schema) ProcessRow(schemaName string, tableName string, col Column) {
	if _, ok := s.Tables[tableName]; !ok {
		s.Tables[tableName] = Table{
			Schema:  schemaName,
			Name:    tableName,
			Columns: []Column{},
		}
	}

	t := s.Tables[tableName]
	t.Columns = append(t.Columns, col)
	s.Tables[tableName] = t
}

var (
	flagDatabaseURL = flag.String("database-url", os.Getenv("DATABASE_URL"), "Database URL to connect to")
	flagConfigPath  = flag.String("config", "pginspector.yaml", "Path to config file")
	flagOutputPath  = flag.String("output", "generated.sql", "Path to output file")
	flagAction      = flag.String("action", "generate", "Action to perform (generate, inspect, or help)")
	flagDebug       = flag.Bool("debug", false, "Enable debug logging")
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	flag.Parse()

	databaseURL := *flagDatabaseURL
	configPath := *flagConfigPath
	outputPath := *flagOutputPath
	action := *flagAction
	debug := *flagDebug

	if databaseURL == "" {
		log.Fatalf("-database-url (or DATABASE_URL environment variable) must be set (no default assumed)")
	}
	if configPath == "" {
		log.Fatalf("-config must not be empty if set (defaults to pginspector.yaml when not set)")
	}
	if outputPath == "" {
		log.Fatalf("-output must not be empty if set (defaults to generated.sql when not set)")
	}

	if action == "help" {
		fmt.Println("Usage: pginspector -database-url <database-url> -config <config-path> -output <output-path> -action <action>")
		fmt.Println("Actions:")
		fmt.Println("  generate: Generate SQL from a configuration file")
		fmt.Println("  inspect: Inspect a schema and print it to stdout (outputs in configuration file format). Pass the schema name as the first argument.")
		fmt.Println("  help: Print this help message")
		return
	}

	if action == "inspect" {
		schemaName := flag.Arg(0)
		if schemaName == "" {
			log.Printf("Assuming schema \"public\" since no schema name was provided (pass the schema name as the first argument to override)\n")
			schemaName = "public"
		}
		schema, err := inspectTablesInSchema(ctx, databaseURL, schemaName, []string{}, debug)
		if err != nil {
			log.Fatalf("Unable to inspect schema: %v\n", err)
		}

		if len(schema.Tables) == 0 {
			log.Fatalf("No tables found in schema %s\n", schemaName)
		}

		tableConfig := map[string]TableConfig{}
		for tableName := range schema.Tables {
			tableConfig[tableName] = TableConfig{
				ProtoName:               fmt.Sprintf("foo.v1.%s", strcase.ToCamel(tableName)),
				PrimaryKey:              "id",
				GenerateFieldMaskUpdate: true,
			}
		}

		outputConfig := GeneratorConfiguration{
			SchemaConfig: map[string]SchemaConfig{
				schemaName: {
					SkipTables: []string{
						"\"add tables to skip here (likely a migrations table)\"",
					},
					DefaultPrimaryKeyColumn: "id",
					TableConfig:             tableConfig,
				},
			},
		}

		_, err = fmt.Fprintf(os.Stdout, "# And example configuration for the provided database follows.\n# You may need to edit this to suit your needs.\n\n")
		if err != nil {
			log.Fatalf("Unable to write output to stdout: %v\n", err)
		}
		err = yaml.NewEncoder(os.Stdout).Encode(outputConfig)
		if err != nil {
			log.Fatalf("Unable to encode schema: %v\n", err)
		}
		return
	}

	cfgReader, err := os.Open(configPath)
	if err != nil {
		log.Fatalf("Unable to open config file: %v\n", err)
	}
	cfg, err := ReadConfig(cfgReader)
	if err != nil {
		log.Fatalf("Unable to read config file: %v\n", err)
	}

	outputBuffer := bytes.NewBuffer([]byte{})

	err = generate(ctx, databaseURL, cfg, outputBuffer, debug)
	if err != nil {
		log.Fatalf("Unable to generate SQL: %v\n", err)
	}

	if outputPath == "-" {
		_, err = io.Copy(os.Stdout, outputBuffer)
		if err != nil {
			log.Fatalf("Unable to write output to stdout: %v\n", err)
		}
	} else {
		err = os.WriteFile(outputPath, outputBuffer.Bytes(), 0644)
		if err != nil {
			log.Fatalf("Unable to write output to file: %v\n", err)
		}
	}
}

func generate(ctx context.Context, databaseURL string, cfg GeneratorConfiguration, outputBuffer io.Writer, debug bool) error {
	_, err := fmt.Fprintf(outputBuffer, "-- File generated by pginspector. DO NOT EDIT.\n\n")
	if err != nil {
		return errors.WithMessage(err, "Unable to write output to file")
	}

	sortedSchemaNames := make([]string, 0, len(cfg.SchemaConfig))
	for schemaName := range cfg.SchemaConfig {
		sortedSchemaNames = append(sortedSchemaNames, schemaName)
	}
	sort.Strings(sortedSchemaNames)

	for _, schemaName := range sortedSchemaNames {
		schemaConfig := cfg.SchemaConfig[schemaName]

		inspectedSchema, err := inspectTablesInSchema(ctx, databaseURL, schemaName, schemaConfig.SkipTables, debug)
		if err != nil {
			return errors.WithMessage(err, "Unable to inspect schema")
		}
		tableConfigs := make([]GenerationTable, 0, len(schemaConfig.TableConfig))

		sortedTableNames := make([]string, 0, len(inspectedSchema.Tables))
		for tableName := range inspectedSchema.Tables {
			sortedTableNames = append(sortedTableNames, tableName)
		}
		sort.Strings(sortedTableNames)

		for _, tableName := range sortedTableNames {
			tableConfig := schemaConfig.GetTableConfig(tableName)

			if schemaConfig.ShouldSkipTable(tableName) {
				continue
			}
			inspectedTable, ok := inspectedSchema.Tables[tableName]
			if !ok {
				return errors.Errorf("Unable to find table %s.%s\n", schemaName, tableName)
			}
			if tableConfig.PrimaryKey == "" {
				tableConfig.PrimaryKey = schemaConfig.DefaultPrimaryKeyColumn
			}
			if tableConfig.PrimaryKey == "" {
				return errors.Errorf("No primary key specified for table %s.%s and no default primary key set\n", schemaName, tableName)
			}
			tableConfigs = append(tableConfigs, GenerationTable{
				Table:  inspectedTable,
				Config: tableConfig,
			})
		}

		err = generateGetAndListQueries(ctx, outputBuffer, tableConfigs)
		if err != nil {
			return errors.WithMessage(err, "Unable to generate get and list queries")
		}

		err = generateUpdateQueries(ctx, outputBuffer, tableConfigs)
		if err != nil {
			return errors.WithMessage(err, "Unable to generate update queries")
		}
	}

	return nil
}

type logger struct{}

func (l *logger) Log(ctx context.Context, level pgx.LogLevel, msg string, data map[string]interface{}) {
	if data != nil {
		log.Printf("%s: %s: %v", level, msg, data)
		return
	}
	log.Printf("%s: %s", level, msg)
}

func inspectTablesInSchema(ctx context.Context, dbConnectionString string, schemaName string, excludedTableNames []string, debug bool) (Schema, error) {
	pgxConfig, err := pgxpool.ParseConfig(dbConnectionString)
	if err != nil {
		log.Fatalf("Unable to parse database connection string: %v\n", err)
	}
	if debug {
		pgxConfig.ConnConfig.Logger = &logger{}
	}
	pool, err := pgxpool.ConnectConfig(ctx, pgxConfig)
	if err != nil {
		log.Fatalf("Unable to create connection pool: %v\n", err)
	}

	querier := models.NewQuerier(pool)

	tablesAndColumns, err := querier.ListTableColumnsInSchema(ctx, schemaName)
	if err != nil {
		log.Fatal(err)
	}

	sch := Schema{
		Tables: map[string]Table{},
	}

	for _, col := range tablesAndColumns {
		for _, excludedTableName := range excludedTableNames {
			if col.TableName == excludedTableName {
				continue
			}
		}
		sch.ProcessRow(schemaName, col.TableName, Column{
			Name:     col.ColumnName,
			PGType:   Unwrap(col.DataType),
			Nullable: Unwrap(col.IsNullable) == "YES",
			Default:  Unwrap(col.ColumnDefault),
		})
	}

	if debug {
		for _, table := range sch.Tables {
			table.PrettyPrint()
		}
	}

	return sch, nil
}

func generateGetAndListQueries(ctx context.Context, w io.Writer, tables []GenerationTable) error {
	tmpl, err := template.New("SQLGetAndListQueries").Funcs(template.FuncMap{
		"ToCamel": strcase.ToCamel,
	}).Parse(`{{- define "SQLGetAndListQueries" -}}
{{- range . }}

-- name: Select{{ ToCamel .Name }}ByID :one {{- if .Config.ProtoName }} proto-type={{ .Config.ProtoName }} {{- end }}
SELECT
        {{- range $index, $col := .Columns }}
        {{- if $index}},{{ end }}
        {{ $col.Name }}
        {{- end }}
FROM {{ .Schema }}.{{ .Name }}
WHERE {{ .Config.PrimaryKey }} = pggen.arg('{{ .Config.PrimaryKey }}');

-- name: Select{{ ToCamel .Name }}List :many {{- if .Config.ProtoName }} proto-type={{ .Config.ProtoName }} {{- end }}
SELECT
        {{- range $index, $col := .Columns }}
        {{- if $index}},{{ end }}
        {{ $col.Name }}
        {{- end }}
FROM {{ .Schema }}.{{ .Name }};

{{- end }}
{{- end }}
`)
	if err != nil {
		return err
	}
	return tmpl.Execute(w, tables)
}

func generateUpdateQueries(ctx context.Context, w io.Writer, tables []GenerationTable) error {
	tmpl, err := template.New("SQLUpdateQueries").Funcs(template.FuncMap{
		"ToCamel": strcase.ToCamel,
	}).Parse(`{{- define "SQLUpdateQueries" -}}
{{- range . }}

-- name: Update{{ ToCamel .Name }} :one {{- if .Config.ProtoName }} proto-type={{ .Config.ProtoName }} {{- end }}
UPDATE {{ .Schema }}.{{ .Name }}
SET (
{{- range $index, $col := .Columns }}
        {{- if $index}},{{ end }}
        {{ $col.Name }}
        {{- end }}
) = (
{{- range $index, $col := .Columns }}
        {{- if $index}},{{ end }}
        pggen.arg('{{ $col.Name }}')
        {{- end }}
) WHERE {{ .Config.PrimaryKey }} = pggen.arg('{{ .Config.PrimaryKey }}') RETURNING *;

{{- if .Config.GenerateFieldMaskUpdate }}
-- name: Update{{ ToCamel .Name }}FieldMask :one {{- if .Config.ProtoName }} proto-type={{ .Config.ProtoName }} {{- end }}
UPDATE {{ .Schema }}.{{ .Name }}
SET (
{{- range $index, $col := .Columns }}
        {{- if $index}},{{ end }}
        {{ $col.Name }}
        {{- end }}
) = (
{{- range $index, $col := .Columns }}
        {{- if $index}},{{ end }}
        CASE
        	WHEN '{{ $col.Name }}' = ANY(pggen.arg('_field_mask')::text[]) THEN pggen.arg('{{ $col.Name }}')
        	ELSE {{ $col.Name }}
        END
        {{- end }}
) WHERE {{ .Config.PrimaryKey }} = pggen.arg('{{ .Config.PrimaryKey }}') RETURNING *;
{{- end }}

{{- end }}
{{- end }}`)
	if err != nil {
		return err
	}
	return tmpl.Execute(w, tables)
}

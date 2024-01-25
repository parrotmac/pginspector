package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v4"
	satoriuuid "github.com/satori/go.uuid"
)

type InspectedColumn struct {
	Name string
	Type string

	PointsToTable  string
	PointsToColumn string

	OrdinalPosition int
}

const (
	CommonTypeNameString = "string"
	CommonTypeNameInt64  = "int64"
)

type InspectedTable struct {
	Name    string
	Columns []InspectedColumn
}

func (it *InspectedTable) GetColumnByName(name string) InspectedColumn {
	for idx := range it.Columns {
		if it.Columns[idx].Name == name {
			return it.Columns[idx]
		}
	}
	panic("Could not find matching column")
}

// Lists the names of the other tables that this table points to
func (is *InspectedTable) ListTablesPointedTo() []string {
	res := []string{}
	for _, c := range is.Columns {
		if c.PointsToTable != "" {
			res = append(res, c.PointsToTable)
		}
	}
	return res
}

func (is *InspectedTable) HasPointerToColumn(tableName, columnName string) bool {
	for _, col := range is.Columns {
		if col.PointsToTable == tableName && col.PointsToColumn == columnName {
			return true
		}
	}
	return false
}

func (is *InspectedTable) HasPointerToTable(tableName string) *InspectedColumn {
	for _, col := range is.Columns {
		if col.PointsToTable == tableName {
			return &col
		}
	}
	return nil
}

type ResultSet struct {
	Tables []InspectedTable
}

func (rs *ResultSet) GetTableIndexByName(name string) int {
	for idx := range rs.Tables {
		if rs.Tables[idx].Name == name {
			return idx
		}
	}
	return -1
}

func (rs *ResultSet) AddTable(table InspectedTable) {
	for _, t := range rs.Tables {
		if t.Name == table.Name {
			panic("Cannot add multiple tables with the same name")
		}
	}
	rs.Tables = append(rs.Tables, table)
}

func (rs *ResultSet) DetermineInsertTableOrder() []string {
	res := []string{}

	for _, t := range rs.Tables {
		pointsTo := t.ListTablesPointedTo()

		// Has no dependencies, and not already in result set
		if len(pointsTo) == 0 && !contains(res, t.Name) {
			res = append(res, t.Name)
			continue
		}
		allDepsAvailable := true
		for _, dep := range pointsTo {
			if !contains(pointsTo, dep) {
				allDepsAvailable = false
				break
			}
		}
		if allDepsAvailable {
			res = append(res, t.Name)
			continue
		}
	}

	return res
}

type Column struct {
	TableCatalog    string  `sql:"table_catalog"`
	TableSchema     string  `sql:"table_schema"`
	TableName       string  `sql:"table_name"`
	ColumnName      string  `sql:"column_name"`
	OrdinalPosition int     `sql:"ordinal_position"`
	ColumnDefault   *string `sql:"column_default"`
	IsNullable      string  `sql:"is_nullable"`
	DataType        string  `sql:"data_type"`
}

const ColumnQuery = `SELECT table_catalog,
table_schema,
table_name,
column_name,
ordinal_position,
column_default,
is_nullable,
data_type
FROM 
information_schema.columns WHERE table_schema = 'public';`

func getColumns(ctx context.Context, conn *pgx.Conn) ([]Column, error) {
	rows, err := conn.Query(ctx, ColumnQuery)
	if err != nil {
		return nil, err
	}
	columnRows := []Column{}
	for rows.Next() {
		c := Column{}
		err := rows.Scan(&c.TableCatalog, &c.TableSchema, &c.TableName, &c.ColumnName, &c.OrdinalPosition, &c.ColumnDefault, &c.IsNullable, &c.DataType)
		if err != nil {
			return nil, err
		}
		columnRows = append(columnRows, c)
	}
	return columnRows, rows.Err()
}

type SchemaInfo struct {
	TableSchema        string `sql:"table_schema"`
	ConstraintName     string `sql:"constraint_name"`
	TableName          string `sql:"table_name"`
	ColumnName         string `sql:"column_name"`
	ForeignTableSchema string `sql:"foreign_table_schema"`
	ForeignTableName   string `sql:"foreign_table_name"`
	ForeignColumnName  string `sql:"foreign_column_name"`
}

const SchemaInfoQuery = `SELECT
    tc.table_schema, 
    tc.constraint_name, 
    tc.table_name, 
    kcu.column_name, 
    ccu.table_schema AS foreign_table_schema,
    ccu.table_name AS foreign_table_name,
    ccu.column_name AS foreign_column_name 
FROM 
    information_schema.table_constraints AS tc 
    JOIN information_schema.key_column_usage AS kcu
      ON tc.constraint_name = kcu.constraint_name
      AND tc.table_schema = kcu.table_schema
    JOIN information_schema.constraint_column_usage AS ccu
      ON ccu.constraint_name = tc.constraint_name
      AND ccu.table_schema = tc.table_schema
WHERE tc.constraint_type = 'FOREIGN KEY';`

func getSchemaInfo(ctx context.Context, conn *pgx.Conn) ([]SchemaInfo, error) {
	rows, err := conn.Query(ctx, SchemaInfoQuery)
	if err != nil {
		return nil, err
	}
	schemaInfoRows := []SchemaInfo{}
	for rows.Next() {
		s := SchemaInfo{}
		err := rows.Scan(&s.TableSchema, &s.ConstraintName, &s.TableName, &s.ColumnName, &s.ForeignTableSchema, &s.ForeignTableName, &s.ForeignColumnName)
		if err != nil {
			return nil, err
		}
		schemaInfoRows = append(schemaInfoRows, s)
	}
	return schemaInfoRows, rows.Err()
}

func contains(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

func main() {
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close(ctx)

	fullInfo := ResultSet{
		Tables: []InspectedTable{},
	}

	schemaData, err := getSchemaInfo(ctx, conn)
	if err != nil {
		log.Fatalln(err)
	}

	columnData, err := getColumns(ctx, conn)
	if err != nil {
		log.Fatalln(err)
	}

	tableNames := []string{}

	for _, col := range columnData {
		if !contains(tableNames, col.TableName) {
			tableNames = append(tableNames, col.TableName)
		}
	}

	startAtTable := "editor_epdatavariable"

	for _, tableName := range tableNames {
		columnInfo := []InspectedColumn{}

		for _, colData := range columnData {
			if colData.TableName == tableName {
				var ForeignTableName string
				var ForeignColumnName string

				for _, d := range schemaData {
					if d.TableName == tableName && d.ColumnName == colData.ColumnName {
						ForeignTableName = d.ForeignTableName
						ForeignColumnName = d.ForeignColumnName
					}
				}

				columnInfo = append(columnInfo, InspectedColumn{
					Name:            colData.ColumnName,
					Type:            colData.DataType,
					PointsToTable:   ForeignTableName,
					PointsToColumn:  ForeignColumnName,
					OrdinalPosition: colData.OrdinalPosition,
				})
			}
		}

		sort.Slice(columnData, func(i, j int) bool {
			return columnData[i].OrdinalPosition < columnData[j].OrdinalPosition
		})

		fullInfo.AddTable(InspectedTable{
			Name:    tableName,
			Columns: columnInfo,
		})
	}

	// b, err := json.Marshal(fullInfo)
	// if err != nil {
	// 	log.Fatalln(err)
	// }

	// println(string(b))

	// startTable := fullInfo.Tables[fullInfo.GetTableIndexByName(startAtTable)]

	traverseTables(conn, fullInfo, startAtTable, "id", "ce44b234-14d9-4a5f-80c6-1809aab09871")

	// println("--------------------------------------")

	// b, err := json.Marshal(fullContents)
	// if err != nil {
	// 	log.Fatalln(err)
	// }

	// println(string(b))

	// println("--------------------------------------")

	tableOrdering := fullInfo.DetermineInsertTableOrder()

	for _, tableName := range tableOrdering {
		for _, tblInfo := range fullContents {
			if tblInfo.TableName != tableName {
				continue
			}
			quotedColumnNames := []string{}
			for _, colName := range tblInfo.InsertColumnNames {
				quotedColumnNames = append(quotedColumnNames, fmt.Sprintf("\"%s\"", colName))
			}
			valueRows := ""
			for i, r := range tblInfo.ValuesRows {
				valueRows += fmt.Sprintf("(%s)", r)
				if i != len(tblInfo.ValuesRows)-1 {
					valueRows += ",\n"
				}
			}

			stmt := fmt.Sprintf(`INSERT
INTO "public"."%s"(%s)
VALUES
%s
;`, tblInfo.TableName, strings.Join(quotedColumnNames, ","), valueRows)
			println(stmt)
		}

	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "Query failed: %v\n", err)
		os.Exit(1)
	}
}

type tableContents struct {
	TableName         string
	InsertColumnNames []string
	ValuesRows        []string
}

var fullContents = []tableContents{}

func getOrCreateTableContents(tableName string) int {
	for idx, c := range fullContents {
		if c.TableName == tableName {
			return idx
		}
	}
	fullContents = append(fullContents, tableContents{
		TableName: tableName,
	})
	return len(fullContents) - 1
}

var visited = []string{}

func traverseTables(conn *pgx.Conn, resSet ResultSet, fromTable, identifyingColumnName string, identifier interface{}) {
	// Ewwww
	rows, err := conn.Query(context.Background(), fmt.Sprintf("SELECT * FROM %s WHERE %s = '%v' ORDER BY %s DESC LIMIT 1", fromTable, identifyingColumnName, identifier, "id"))
	if err != nil {
		panic(err)
	}

	tabl := resSet.Tables[resSet.GetTableIndexByName(fromTable)]

	ifVals := make([]interface{}, len(tabl.Columns))
	ifValPrts := make([]interface{}, len(tabl.Columns))

	for i := range ifVals {
		ifValPrts[i] = &ifVals[i]
	}

	resultData := map[string]interface{}{}
	if rows.Next() {
		vals := []string{}
		for i, r := range rows.RawValues() {
			colDef := tabl.Columns[i]

			switch colDef.Type {
			case "text", "character varying", "json":
				ifVals[i] = ""
			case "uuid":
				var u satoriuuid.UUID
				// var u [16]uint8
				ifVals[i] = u
			case "timestamp with time zone", "timestamp without time zone":
				ifVals[i] = time.Time{}
			case "integer":
				ifVals[i] = 0
			default:
				ifVals[i] = nil
			}

			// println(colDef.Type, "//", fmt.Sprint(colDef.OrdinalPosition), ":", AsStringFromValue(colDef.Type, r))
			vals = append(vals, string(r))
		}

		// println("Row for", fromTable, ":", strings.Join(vals, ", "))

		err := rows.Scan(ifValPrts...)
		if err != nil {
			panic(err)
		}

		values, err := rows.Values()
		if err != nil {
			panic(err)
		}
		for idx, field := range rows.FieldDescriptions() {
			resultData[string(field.Name)] = values[idx]
		}
	}
	rows.Close()

	colIdx := getOrCreateTableContents(fromTable)
	fullContents[colIdx].InsertColumnNames = make([]string, len(tabl.Columns))
	for i, col := range tabl.Columns {
		fullContents[colIdx].InsertColumnNames[i] = col.Name
	}

	rowStringVals := []string{}

	for i := range ifValPrts {
		e := ifValPrts[i]
		if *e.(*interface{}) == nil {
			rowStringVals = append(rowStringVals, "NULL")
			continue
		}

		switch tabl.Columns[i].Type {
		case "uuid":
			j := *e.(*interface{})
			if j != nil {
				a := fmt.Sprintf("'%+v'", satoriuuid.UUID(j.([16]uint8)))
				rowStringVals = append(rowStringVals, a)
			}
		case "timestamp", "timestamp with time zone", "timestamp without time zone":
			j := (*e.(*interface{})).(time.Time)

			// dateStr := fmt.Sprintf("%+v", *e.(*interface{}))
			// t, err := time.Parse(time.RFC3339, dateStr)
			// if err != nil {
			// 	panic(err)
			// }
			// a := t.Format("2006-01-02T15:04:05.000Z")

			a := j.Format("2006-01-02T15:04:05.000Z")
			rowStringVals = append(rowStringVals, fmt.Sprintf("'%s'", a))
		case "json", "jsonb":
			b, err := json.Marshal(*e.(*interface{}))
			if err != nil {
				panic(err)
			}
			rowStringVals = append(rowStringVals, fmt.Sprintf("'%s'", string(b)))
		default:
			a := fmt.Sprintf("'%+v'", *e.(*interface{}))
			rowStringVals = append(rowStringVals, a)
		}
	}

	rowVal := strings.Join(rowStringVals, ", ")
	fullContents[colIdx].ValuesRows = append(fullContents[colIdx].ValuesRows, rowVal)

	for _, t := range resSet.Tables {
		if col := t.HasPointerToTable(fromTable); col != nil {
			ident := AsStringFromValue(col.Type, resultData[col.PointsToColumn])
			// println("Looking at", t.Name, col.Name, ident)
			traverseTables(conn, resSet, t.Name, col.Name, ident)
		}
	}
	for _, col := range resSet.Tables[resSet.GetTableIndexByName(fromTable)].Columns {
		if col.PointsToTable != "" && col.PointsToColumn != "" {
			data := resultData[col.Name]
			if data == nil {
				continue
			}
			ident := AsStringFromValue(col.Type, data)
			if contains(visited, ident) {
				continue
			} else {
				visited = append(visited, ident)
			}
			// println(col.Name, "points to", col.PointsToTable, col.PointsToColumn, "looking by", ident)
			traverseTables(conn, resSet, col.PointsToTable, col.PointsToColumn, ident)
		}
	}
}

func AsStringFromValue(pgTypeName string, value interface{}) string {
	if value == nil {
		return "<<<<<<<<<<<nil>>>>>>>>>>"
	}
	switch pgTypeName {
	// Group a bunch of types together
	case "text", "character varying", "json":
		str, ok := value.(string)
		if !ok {
			d := []uint8{}
			for i := 0; i < len(value.([]uint8)); i++ {
				d = append(d, value.([]uint8)[i])
			}
			return string(d)
		} else {
			return str
		}
	// case "jsonb":
	// 	return "<jsonb>"
	case "uuid":
		regBytes := make([]byte, 16)
		v, ok := value.([16]uint8)
		if !ok {
			d := value.([]byte)
			for i := range d {
				regBytes[i] = byte(d[i])
			}
		} else {
			for i := range v {
				regBytes[i] = byte(v[i])
			}
		}
		u, err := uuid.FromBytes(regBytes)
		if err != nil {
			panic(err)
		}
		return u.String()
	case "timestamp with time zone", "timestamp without time zone":
		return "<timestamp>"
		// v, err := value.(pgtype.Timestamp).Value()
		// if err != nil {
		// 	panic(err)
		// }
		// return fmt.Sprintf("%v", v)
	case "integer":
		return fmt.Sprintf("%v", value)
	}
	return ""
}

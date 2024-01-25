
-- name: ListTableColumnsInSchema :many
SELECT column_name, data_type, column_default, is_nullable, table_name FROM information_schema.columns WHERE table_schema = pggen.arg('schema_name') ORDER BY column_name;

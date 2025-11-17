package main

var mainTemplateString = `package {{.PackageName}}

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
    "{{.ModuleName}}/m_options"
	"{{.ModuleName}}/sql_builder"
    "{{.ProjectName}}/log/logger"
)

const (
    Package = "{{.PackageName}}"
    Table = "{{.TableName}}"
    ID = "{{.ID}}"
	// Secondary indexes
	{{- range .SecondatyIndexes }}
	{{.IndexName.CamelField}} = "{{.IndexName.Snake}}"
	{{- end }}
)

type Facade struct {
	log *logger.Logger
	db  *pgxpool.Pool
	//
	{{- range .Childs}}
	{{.Camel}} *{{.PackageName}}.Facade
	{{- end}}
}

func New(o *m_options.Options) *Facade {
	return &Facade{
		log: o.Log,
		db:  o.DB,
		//
		{{- range .Childs}}
		{{.Camel}}: {{.PackageName}}.New(o),
		{{- end}}
	}
}

func (f *Facade) logError(functionName string, msg string, h logger.H) {
	f.log.Error(fmt.Sprintf("[%s.%s - %s] %s", Package, functionName, Table, msg), h)
}

type Data struct {
{{- range .Fields}}
	{{.Name}} {{.Type}}
{{- end}}
}

func (data *Data) Map() map[string]any {
	out := make(map[string]any, len(allFieldsList))
	{{- range .Fields}}
	out[string({{.Name}})] = data.{{.Name}}
	{{- end}}
	return out
}


type Field string

const (
{{- range .Fields}}
    {{.Name}} Field = "{{.Snake}}"
{{- end}}
)

{{- range .Enums}}
	type {{.Name}} string

	const (
		{{- range .Values}}
			{{.Name}} {{.ConstName}} = "{{.Value}}"
		{{- end}}
	)

	func (e {{.Name}}) String() string {
		return string(e)
	}

	func (e *{{.Name}}) IsValid() bool {
		switch *e {
		case {{.ConstNames}}:
			return true
		}
		return false
	}
{{- end}}

func GetAllFields() []Field {
   return []Field{
	 {{- range .Fields}}
     	{{.Name}},
	 {{- end}}
   }
}

var allFieldsList = GetAllFields()

// Helper function to generate PostgreSQL placeholders ($1, $2, ...)
func generatePlaceholders(n int) string {
	placeholders := make([]string, n)
	for i := 0; i < n; i++ {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}
	return strings.Join(placeholders, ", ")
}

func (f Field) String() string {
    return string(f)
}

var fieldsMap = map[Field]func(data *Data) interface{}{
{{- range .Fields}}
            {{.Name}}: func(data *Data) interface{} { return &data.{{.Name}} },
{{- end}}
}

func (data *Data) fieldPtrs(fields []Field) []interface{} {
	ptrs := make([]interface{}, len(fields))

	for i, field := range fields {
        ptrs[i] =  fieldsMap[field](data)
    }
    return ptrs
}

type PrimaryKey struct {
	{{- range .PrimaryKeys }}
	{{.CamelField}} {{.Type}}
	{{- end }}
}

func GetColumns() []string {
    return []string{
{{- range .Fields}}
        {{.Name}}.String(),
{{- end}}
    }
}

var allStringFields = GetColumns()

func GetValues(data *Data) []interface{} {
    return []interface{}{
{{- range .Fields}}
        data.{{.Name}},
{{- end}}
    }
}

type UpdateFields map[Field]interface{}

func (uf UpdateFields) Map() map[string]any {
	out := make(map[string]any, len(uf))
	for k, v := range uf {
		out[string(k)] = v
	}
	return out
}

type Op string

const (
	OpEq    Op = "="      // Equal
	OpNe    Op = "!="     // Not Equal
	OpIn    Op = "IN"     // In
	OpLt    Op = "<"      // Less than
	OpGt    Op = ">"      // Greater than
	OpLe    Op = "<="     // Less than or equal
	OpGe    Op = ">="     // Greater than or equal
	OpIs    Op = "IS"     // Is (null)
	OpIsNot Op = "IS NOT" // Is not (null)
)

type QueryParam struct {
	Field    Field
	Operator Op
	Value    interface{}
}

func makeStringFields(fields []Field) []string {
	stringFields := make([]string, len(fields))
	for i, f := range fields {
		stringFields[i] =` + "\"`\"" + ` + string(f) +` + "\"`\"" +
	`}

	return stringFields
}

// nil selects all fields
func SelectQuery(fields []Field) string {
	var stringFields []string
	if fields == nil || len(fields) == 0 {
		stringFields = makeStringFields(allFieldsList)
	} else {
		stringFields = makeStringFields(fields)
	}

	queryString := fmt.Sprintf("SELECT %s FROM %s",
		strings.Join(stringFields, ", "), Table)

	return queryString
}

func ConstructWhereClause(queryParams []QueryParam) (whereClause string, params map[string]interface{}) {
	whereClauses := make([]string, len(queryParams))
	params =  make(map[string]interface{}, len(queryParams))
	builder := strings.Builder{}
	for i, qp := range queryParams {
	if (qp.Operator == OpIs || qp.Operator == OpIsNot) && qp.Value == nil {
        whereClauses[i] = fmt.Sprintf("%s %s NULL", qp.Field, qp.Operator)
        continue
    }
	builder.WriteString("param")
		builder.WriteString(strconv.Itoa(i))
		paramName := builder.String()
		builder.Reset()

		// Construct param
		builder.WriteString("@")
		builder.WriteString(paramName)
		param := builder.String()
		builder.Reset()

		if qp.Operator == "IN" {
			builder.WriteString("UNNEST(")
			builder.WriteString(param)
			builder.WriteString(")")
			param = builder.String()
			builder.Reset()
		}

		// Construct whereClause
		builder.WriteString("` + "`" + `")
		builder.WriteString(string(qp.Field))
		builder.WriteString("` + "`" + `")
		builder.WriteString(" ")
		builder.WriteString(string(qp.Operator))
		builder.WriteString(" ")
		builder.WriteString(param)
		whereClause := builder.String()
		whereClauses[i] = whereClause
		params[paramName] = qp.Value
		builder.Reset()
	}

	return strings.Join(whereClauses, " AND "), params
}
`

var oldTemplateString = `

func (f *Facade) Create(ctx context.Context, data *Data) error {
	columns := GetColumns()
	values := GetValues(data)

	query := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s)",
		Table,
		strings.Join(columns, ", "),
		generatePlaceholders(len(columns)),
	)

	_, err := f.db.Exec(ctx, query, values...)
	if err != nil {
		f.logError("Create", "Failed to insert", logger.H{
			"error": err,
			"data":  data,
		})
		return err
	}

	return nil
}

func (f *Facade) CreateOrUpdate(ctx context.Context, data *Data) error {
	columns := GetColumns()
	values := GetValues(data)

	updateCols := make([]string, 0, len(columns))
	for _, col := range columns {
		updateCols = append(updateCols, fmt.Sprintf("%s = EXCLUDED.%s", col, col))
	}

	query := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s) ON CONFLICT ({{range $i, $pk := .PrimaryKeys}}{{if $i}}, {{end}}{{$pk.Snake}}{{end}}) DO UPDATE SET %s",
		Table,
		strings.Join(columns, ", "),
		generatePlaceholders(len(columns)),
		strings.Join(updateCols, ", "),
	)

	_, err := f.db.Exec(ctx, query, values...)
	if err != nil {
		f.logError("CreateOrUpdate", "Failed to upsert", logger.H{
			"error": err,
			"data":  data,
		})
		return err
	}

	return nil
}

func (f *Facade) Exists(
    ctx context.Context,
{{- range .PrimaryKeys }}
    {{.Camel }} {{.Type}},
{{- end }}
) bool {
	query := "SELECT 1 FROM " + Table + " WHERE {{.ID}} = $1 LIMIT 1"

	var exists int
	err := f.db.QueryRow(ctx, query, {{.ID}}).Scan(&exists)
	return err == nil
}

func (f *Facade) ExistsTx(
	ctx context.Context,
	tx pgx.Tx,
{{- range .PrimaryKeys }}
    {{.Camel }} {{.Type}},
{{- end }}
) bool {
	query := "SELECT 1 FROM " + Table + " WHERE {{.ID}} = $1 LIMIT 1"

	var exists int
	err := tx.QueryRow(ctx, query, {{.ID}}).Scan(&exists)
	return err == nil
}

func (f *Facade) Get(
	ctx context.Context,
	queryParams []QueryParam,
	fields []Field,
) ([]*Data, error) {
	// Construct SQL query
	queryString := SelectQuery(fields)
	whereClauses, params := ConstructWhereClause(queryParams)
	if len(queryParams) > 0 {
		queryString += " WHERE " + whereClauses
	}

	// Convert params map to array for PostgreSQL
	paramValues := make([]interface{}, 0, len(params))
	for _, v := range params {
		paramValues = append(paramValues, v)
	}

	rows, err := f.db.Query(ctx, queryString, paramValues...)
	if err != nil {
		f.logError("Get", "Failed to query", logger.H{
			"error":        err,
			"query_params": queryParams,
			"fields":       fields,
		})
		return nil, err
	}
	defer rows.Close()

	res := make([]*Data, 0)

	for rows.Next() {
		var data Data
		if err := rows.Scan(data.fieldPtrs(fields)...); err != nil {
			f.logError("Get", "Failed to Scan", logger.H{
				"error":        err,
				"query_params": queryParams,
				"fields":       fields,
			})
			return nil, err
		}
		res = append(res, &data)
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}

	return res, nil
}

func (f *Facade) GetTx(
	ctx context.Context,
	tx pgx.Tx,
	queryParams []QueryParam,
	fields []Field,
) ([]*Data, error) {
	// Construct SQL query
	queryString := SelectQuery(fields)
	whereClauses, params := ConstructWhereClause(queryParams)
	if len(queryParams) > 0 {
		queryString += " WHERE " + whereClauses
	}

	// Convert params map to array for PostgreSQL
	paramValues := make([]interface{}, 0, len(params))
	for _, v := range params {
		paramValues = append(paramValues, v)
	}

	rows, err := tx.Query(ctx, queryString, paramValues...)
	if err != nil {
		f.logError("GetTx", "Failed to query", logger.H{
			"error":        err,
			"query_params": queryParams,
			"fields":       fields,
		})
		return nil, err
	}
	defer rows.Close()

	res := make([]*Data, 0)

	for rows.Next() {
		var data Data
		if err := rows.Scan(data.fieldPtrs(fields)...); err != nil {
			f.logError("GetTx", "Failed to Scan", logger.H{
				"error":        err,
				"query_params": queryParams,
				"fields":       fields,
			})
			return nil, err
		}
		res = append(res, &data)
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}

	return res, nil
}


func (f *Facade) Find(
	ctx context.Context,
{{- range .PrimaryKeys }}
    {{.Camel }} {{.Type}},
{{- end }}
    fields []Field,
) (*Data, error) {
	stringFields := make([]string, len(fields))
	for i, f := range fields {
		stringFields[i] = string(f)
	}

	row, err := f.db.Single().ReadRow(
        ctx,
        Table,
        spanner.Key{
            {{- range .PrimaryKeys }}
            {{ .Camel }},
            {{- end }}
        },
        stringFields,
    )
    if err != nil {
		f.logError("Find", "Failed to ReadRow", logger.H{
			"error":           err,
		{{- range .PrimaryKeys }}
            "{{.Snake}}": {{.Camel}},
        {{- end }}
			"fields":          fields,
		})
		return nil, err
	}

	var data Data

	err = row.Columns(data.fieldPtrs(fields)...)
	if err != nil {
		f.logError("Find", "Failed to Scan", logger.H{
			"error":  err,
            {{- range .PrimaryKeys }}
            "{{.Snake}}": {{.Camel}},
            {{- end }}
			"fields": fields,
		})
		return nil, err
	}

	return &data, nil
}

func (f *Facade) FindRtx(
	ctx context.Context,
	rtx *spanner.ReadOnlyTransaction,
{{- range .PrimaryKeys }}
    {{.Camel }} {{.Type}},
{{- end }}
    fields []Field,
) (*Data, error) {
	stringFields := make([]string, len(fields))
	for i, f := range fields {
		stringFields[i] = string(f)
	}

    row, err := rtx.ReadRow(
        ctx,
        Table,
        spanner.Key{
            {{- range .PrimaryKeys }}
            {{ .Camel }},
            {{- end }}
        },
        stringFields,
    )
    if err != nil {
        f.logError("Find", "Failed to ReadRow", logger.H{
            "error":           err,
        {{- range .PrimaryKeys }}
            "{{.Snake}}": {{.Camel}},
        {{- end }}
            "fields":          fields,
        })
        return nil, err
    }

    var data Data

    err = row.Columns(data.fieldPtrs(fields)...)
    if err != nil {
        f.logError("Find", "Failed to Scan", logger.H{
            "error":  err,
            {{- range .PrimaryKeys }}
            "{{.Snake}}": {{.Camel}},
            {{- end }}
            "fields": fields,
        })
        return nil, err
    }

    return &data, nil
}

func (f *Facade) Retrieve(
	ctx context.Context,
{{- range .PrimaryKeys }}
	{{.Camel }} {{.Type}},
{{- end }}
) (*Data, error) {
	return f.Find(ctx, {{- range .PrimaryKeys }}{{.Camel}}, {{- end }}allFieldsList)
}

func (f *Facade) RetrieveRtx(
	ctx context.Context,
	rtx *spanner.ReadOnlyTransaction,
{{- range .PrimaryKeys }}
	{{.Camel }} {{.Type}},
{{- end }}
) (*Data, error) {
	return f.FindRtx(ctx, rtx, {{- range .PrimaryKeys }}{{.Camel}}, {{- end }}allFieldsList)
}

func (f *Facade) CreateTx(
    ctx context.Context,
    tx *spanner.ReadWriteTransaction,
    data *Data,
) error {
    mut := spanner.Insert(Table, GetColumns(), GetValues(data))

    if err := tx.BufferWrite([]*spanner.Mutation{mut}); err != nil {
        f.logError("CreateTx", "Failed to BufferWrite", logger.H{
            "error": err, "data": data,
        })
        return err
    }
    return nil
}

func (f *Facade) UpdateTx(
    ctx context.Context,
    tx *spanner.ReadWriteTransaction,
    {{- range .PrimaryKeys }}
    {{.Camel }} {{.Type}},
    {{- end }}
    data UpdateFields,
) error {
    mut := f.UpdateMut(
        {{- range .PrimaryKeys }}
        {{.Camel }},
        {{- end }}
        data,
    )

    if err := tx.BufferWrite([]*spanner.Mutation{mut}); err != nil {
        f.logError("UpdateTx", "Failed to BufferWrite", logger.H{
            "error": err, "primaryKeys": map[string]interface{}{
                {{- range .PrimaryKeys }}
                "{{.Snake}}": {{.Camel}}, 
                {{- end }}
            },
            "data": data,
        })
        return err
    }
    return nil
}

func (f *Facade) FindTx(
    ctx context.Context,
    tx *spanner.ReadWriteTransaction,
    {{- range .PrimaryKeys }}
    {{.Camel }} {{.Type}},
    {{- end }}
    fields []Field,
) (*Data, error) {
    strCols := make([]string, len(fields))
    for i, fld := range fields {
        strCols[i] = string(fld)
    }

    row, err := tx.ReadRow(
        ctx,
        Table,
        spanner.Key{
            {{- range .PrimaryKeys }}
            {{ .Camel }},
            {{- end }}
        },
        strCols,
    )
    if err != nil {
        f.logError("FindTx", "Failed to ReadRow", logger.H{
            "error": err,
            {{- range .PrimaryKeys }}
            "{{.Snake}}": {{.Camel}},
            {{- end }}
            "fields": fields,
        })
        return nil, err
    }
    var d Data
    if err := row.Columns(d.fieldPtrs(fields)...); err != nil {
        f.logError("FindTx", "Failed to Scan", logger.H{
            "error": err,
            "fields": fields,
        })
        return nil, err
    }
    return &d, nil
}

func (c *Facade) GetByBuilder(ctx context.Context, builder *sql_builder.Builder[Field]) ([]*Data, error) {
	if builder == nil {
		return nil, fmt.Errorf("builder cannot be nil")
	}
    queryStr := builder.String()
    queryParams := builder.Params()
	fields := builder.Fields()

    stmt := spanner.Statement{
        SQL:    queryStr,
        Params: queryParams,
    }

    iter := c.db.Single().Query(ctx, stmt)
	defer iter.Stop()

	res := make([]*Data, 0, iter.RowCount)

	if err := iter.Do(func(row *spanner.Row) error {
		var data Data

		if err := row.Columns(data.fieldPtrs(fields)...); err != nil {
			c.logError("GetByBuilder", "Failed to Scan", logger.H{
				"error":    err,
				"fields":   fields,
			})
			return err
		}

		res = append(res, &data)

		return nil
	}); err != nil {
		return nil, err
	}

	return res, nil
}


func (c *Facade) GetByBuilderRtx(ctx context.Context, rtx *spanner.ReadOnlyTransaction, builder *sql_builder.Builder[Field]) ([]*Data, error) {
	if builder == nil {
		return nil, fmt.Errorf("builder cannot be nil")
	}
	queryStr := builder.String()
	queryParams := builder.Params()
	fields := builder.Fields()

	stmt := spanner.Statement{
		SQL:    queryStr,
		Params: queryParams,
	}

	iter := rtx.Query(ctx, stmt)
	defer iter.Stop()

	res := make([]*Data, 0, iter.RowCount)

	if err := iter.Do(func(row *spanner.Row) error {
		var data Data

		if err := row.Columns(data.fieldPtrs(fields)...); err != nil {
			c.logError("GetByBuilderRtx", "Failed to Scan", logger.H{
				"error":    err,
				"fields":   fields,
			})
			return err
		}

		res = append(res, &data)

		return nil
	}); err != nil {
		return nil, err
	}

	return res, nil
}

func (c *Facade) GetByBuilderTx(ctx context.Context, tx *spanner.ReadWriteTransaction, builder *sql_builder.Builder[Field]) ([]*Data, error) {
	if builder == nil {
		return nil, fmt.Errorf("builder cannot be nil")
	}
	queryStr := builder.String()
	queryParams := builder.Params()
	fields := builder.Fields()

	stmt := spanner.Statement{
		SQL:    queryStr,
		Params: queryParams,
	}

	iter := tx.Query(ctx, stmt)
	defer iter.Stop()

	res := make([]*Data, 0, iter.RowCount)

	if err := iter.Do(func(row *spanner.Row) error {
		var data Data

		if err := row.Columns(data.fieldPtrs(fields)...); err != nil {
			c.logError("GetByBuilderTx", "Failed to Scan", logger.H{
				"error":    err,
				"fields":   fields,
			})
			return err
		}

		res = append(res, &data)

		return nil
	}); err != nil {
		return nil, err
	}

	return res, nil
}

func (c *Facade) GetByBuilderIter(ctx context.Context, builder *sql_builder.Builder[Field], callback func(*Data)) error {
	if builder == nil {
		return fmt.Errorf("builder cannot be nil")
	}
	queryStr := builder.String()
	queryParams := builder.Params()
	fields := builder.Fields()

	stmt := spanner.Statement{
		SQL:    queryStr,
		Params: queryParams,
	}

	if err := c.db.Single().Query(ctx, stmt).Do(func(row *spanner.Row) error {
		var data Data

		if err := row.Columns(data.fieldPtrs(fields)...); err != nil {
			c.logError("GetByBuilderIter", "Failed to Scan", logger.H{
				"error":    err,
				"fields":   fields,
			})
			return err
		}

		callback(&data)

		return nil
	}); err != nil {
		return err
	}

	return nil
}

func (c *Facade) GetByBuilderRtxIter(ctx context.Context, rtx *spanner.ReadOnlyTransaction, builder *sql_builder.Builder[Field], callback func(*Data)) error {
	if builder == nil {
		return fmt.Errorf("builder cannot be nil")
	}
	queryStr := builder.String()
	queryParams := builder.Params()
	fields := builder.Fields()

	stmt := spanner.Statement{
		SQL:    queryStr,
		Params: queryParams,
	}

	if err := rtx.Query(ctx, stmt).Do(func(row *spanner.Row) error {
		var data Data

		if err := row.Columns(data.fieldPtrs(fields)...); err != nil {
			c.logError("GetByBuilderRtxIter", "Failed to Scan", logger.H{
				"error":    err,
				"fields":   fields,
			})
			return err
		}

		callback(&data)

		return nil
	}); err != nil {
		return err
	}

	return nil
}

func (c *Facade) GetByBuilderTxIter(ctx context.Context, tx *spanner.ReadWriteTransaction, builder *sql_builder.Builder[Field], callback func(*Data)) error {
	if builder == nil {
		return fmt.Errorf("builder cannot be nil")
	}
	queryStr := builder.String()
	queryParams := builder.Params()
	fields := builder.Fields()

	stmt := spanner.Statement{
		SQL:    queryStr,
		Params: queryParams,
	}

	if err := tx.Query(ctx, stmt).Do(func(row *spanner.Row) error {
		var data Data

		if err := row.Columns(data.fieldPtrs(fields)...); err != nil {
			c.logError("GetByBuilderTxIter", "Failed to Scan", logger.H{
				"error":    err,
				"fields":   fields,
			})
			return err
		}

		callback(&data)

		return nil
	}); err != nil {
		return err
	}

	return nil
}

func (f *Facade) GetTx(
    ctx context.Context,
    tx *spanner.ReadWriteTransaction,
    queryParams []QueryParam,
    fields []Field,
) ([]*Data, error) {
    q := SelectQuery(fields)
    whereClause, params := ConstructWhereClause(queryParams)
    if len(queryParams) > 0 {
        q += " WHERE " + whereClause
    }
    stmt := spanner.Statement{SQL: q, Params: params}

    iter := tx.Query(ctx, stmt)
    defer iter.Stop()

    var res []*Data
    if err := iter.Do(func(row *spanner.Row) error {
        var d Data
        if err := row.Columns(d.fieldPtrs(fields)...); err != nil {
            f.logError("GetTx", "Failed to Scan", logger.H{
                "error":        err,
                "queryParams":  queryParams,
                "fields":       fields,
            })
            return err
        }
        res = append(res, &d)
        return nil
    }); err != nil {
        return nil, err
    }

    return res, nil
}

func (f *Facade) ExistTx(
    ctx context.Context,
    tx *spanner.ReadWriteTransaction,
    {{- range .PrimaryKeys }}
    {{.Camel}} {{.Type}},
    {{- end }}
) bool {
    _, err := tx.ReadRow(
        ctx,
        Table,
        spanner.Key{
            {{- range .PrimaryKeys }}
            {{.Camel}},
            {{- end }}
        },
        []string{string(ID)},
    )
    return err == nil
}

func (c *Facade) InitBuilder() *sql_builder.Builder[Field] {
    b := sql_builder.New[Field]("")
    b.Select(allFieldsList...).From(Table)
    return b
}

func (f *Facade) UpdateMut(
	{{- range .PrimaryKeys }}
	{{.Camel }} {{.Type}},
	{{- end }}
	data UpdateFields,
) *spanner.Mutation {
	mutationData := map[string]interface{}{
	{{- range .PrimaryKeys }}
		{{.CamelField}}.String(): {{.Camel}},
	{{- end }}
	}
	for field, value := range data {
		mutationData[field.String()] = value
	}

	return spanner.UpdateMap(Table, mutationData)
}


func (f *Facade) Update(
	ctx context.Context,
	{{- range .PrimaryKeys }}
	{{.Camel }} {{.Type}},
	{{- end }}
	data UpdateFields,
) error {
	mutation := f.UpdateMut(
		{{- range .PrimaryKeys }}
		{{.Camel }},
		{{- end }}
		data,
	)

	if _, err := f.db.Apply(ctx, []*spanner.Mutation{mutation}); err != nil {
		f.logError("Update", "Failed to Apply", logger.H{
			"error": err,
			"data":  data,
		})
		return fmt.Errorf("failed to update file record: %w", err)
	}

	return nil
}

func (f *Facade) UpdateByParams(
	ctx context.Context,
	queryParams []QueryParam,
	data UpdateFields,
) error {
	// Construct SQL query
	whereClauses, params := ConstructWhereClause(queryParams)
	builder := strings.Builder{}
	setClauses := make([]string, 0, len(data))
	// Construct SET clause
	for field, value := range data {
		builder.WriteString(string(field))
		paramName := builder.String()
		builder.Reset()

		// Construct param
		builder.WriteString("@")
		builder.WriteString(paramName)
		param := builder.String()
		builder.Reset()

		// Construct SET clause
		builder.WriteString("` + "`" + `")
		builder.WriteString(string(field))
		builder.WriteString("` + "`" + ` = ")
		builder.WriteString(param)
		setClause := builder.String()
		setClauses = append(setClauses, setClause)
		params[paramName] = value
	}

	queryString := fmt.Sprintf("UPDATE %s SET %s WHERE %s",
		Table, strings.Join(setClauses, ", "), whereClauses)

	stmt := spanner.Statement{
		SQL:    queryString,
		Params: params,
	}

	if _, err := f.db.PartitionedUpdate(ctx, stmt); err != nil {
		f.logError("UpdateByParams", "Failed to PartitionedUpdate", logger.H{
			"error":        err,
			"query_params": queryParams,
			"data":         data,
		})
		return fmt.Errorf("failed to update file record: %w", err)
	}

	return nil
}

func (f *Facade) DeleteMut(
	{{- range .PrimaryKeys }}
	{{.Camel }} {{.Type}},
	{{- end }}
) *spanner.Mutation {
	return spanner.Delete(Table, spanner.Key{
		{{- range .PrimaryKeys }}
		{{.Camel}},
		{{- end }}
	})
}

func (f *Facade) Delete(
	ctx context.Context,
	{{- range .PrimaryKeys }}
	{{.Camel }} {{.Type}},
	{{- end }}
) error {
	mutation := f.DeleteMut(
		{{- range .PrimaryKeys }}
		{{.Camel }},
		{{- end }}
	)

	if _, err := f.db.Apply(ctx, []*spanner.Mutation{mutation}); err != nil {
		f.logError("Delete", "Failed to Apply", logger.H{
			"error": err,
		})
		return fmt.Errorf("failed to delete file record: %w", err)
	}

	return nil
}

func (f *Facade) GetRtxIter(
	ctx context.Context,
	rtx *spanner.ReadOnlyTransaction,
	queryParams []QueryParam,
	fields []Field,
	callback func(*Data),
) error {
	// Construct SQL query
	queryString := SelectQuery(fields)
	whereClauses, params := ConstructWhereClause(queryParams)
	if len(queryParams) > 0 {
		queryString += " WHERE " + whereClauses
	}

	if err := rtx.Query(ctx, spanner.Statement{
		SQL:	queryString,
		Params:	params,
	}).Do(func(row *spanner.Row) error {
		var data Data
		if err := row.Columns(data.fieldPtrs(fields)...); err != nil {
			f.logError("GetRtxIter", "Failed to Scan", logger.H{
				"error":        err,
				"query_params": queryParams,
				"fields":       fields,
			})
			return err
		}

		callback(&data)

		return nil
	}); err != nil {
		return err	
	}

	return nil
}

func (f *Facade) GetIter(
	ctx context.Context,
	queryParams []QueryParam,
	fields []Field,
	callback func(*Data),
) error {
	// Construct SQL query
	queryString := SelectQuery(fields)
	whereClauses, params := ConstructWhereClause(queryParams)
	if len(queryParams) > 0 {
		queryString += " WHERE " + whereClauses
	}

	if err := f.db.Single().Query(ctx, spanner.Statement{
		SQL:	queryString,
		Params:	params,
	}).Do(func(row *spanner.Row) error {
		var data Data
		if err := row.Columns(data.fieldPtrs(fields)...); err != nil {
			f.logError("GetIter", "Failed to Scan", logger.H{
				"error":        err,
				"query_params": queryParams,
				"fields":       fields,
			})
			return err
		}

		callback(&data)

		return nil
	}); err != nil {
		return err	
	}

	return nil
}

func (f *Facade) GetByPrimaryKeys(
	ctx context.Context,
	primaryKeys []PrimaryKey,
	fields []Field,
) ([]*Data, error) {
	stringFields := make([]string, len(fields))
	for i, f := range fields {
		stringFields[i] =  string(f)
	}

	spk := make([]spanner.Key, len(primaryKeys))
	for i, pk := range primaryKeys {
		spk[i] = spanner.Key{
			{{- range .PrimaryKeys }}
			pk.{{.CamelField}},
			{{- end }}
		}
	}
			

	iter := f.db.Single().Read(ctx, Table, spanner.KeySetFromKeys(spk...), stringFields)
	defer iter.Stop()

	var res []*Data
	if err := iter.Do(func(row *spanner.Row) error {
		var data Data

		if err := row.Columns(data.fieldPtrs(fields)...); err != nil {
			f.logError("GetByPrimaryKeys", "Failed to Scan", logger.H{
				"error":        err,
				"fields":       fields,
			})
			return err
		}

		res = append(res, &data)

		return nil
	}); err != nil {
		return nil, err
	}

	return res, nil
}

func (f *Facade) ListByPrimaryKeys(
	ctx context.Context,
	primaryKeys []PrimaryKey,
) ([]*Data, error) {
	return f.GetByPrimaryKeys(ctx, primaryKeys, allFieldsList)
}

func (f *Facade) GetByPrimaryKeysRtx(
	ctx context.Context,
	rtx *spanner.ReadOnlyTransaction,
	primaryKeys []PrimaryKey,
	fields []Field,
) ([]*Data, error) {
	stringFields := make([]string, len(fields))
	for i, f := range fields {
		stringFields[i] =  string(f)
	}

	spk := make([]spanner.Key, len(primaryKeys))
	for i, pk := range primaryKeys {
		spk[i] = spanner.Key{
			{{- range .PrimaryKeys }}
			pk.{{.CamelField}},
			{{- end }}
		}
	}

	iter := rtx.Read(ctx, Table, spanner.KeySetFromKeys(spk...), stringFields)
	defer iter.Stop()

	var res []*Data
	if err := iter.Do(func(row *spanner.Row) error {
		var data Data
		if err := row.Columns(data.fieldPtrs(fields)...); err != nil {
			f.logError("GetByPrimaryKeys", "Failed to Scan", logger.H{
				"error":        err,
				"fields":       fields,
			})
			return err
		}

		res = append(res, &data)

		return nil
	}); err != nil {
		return nil, err
	}

	return res, nil
}

func (f *Facade) GetByPrimaryKeysTx(
    ctx context.Context,
    tx *spanner.ReadWriteTransaction,
    primaryKeys []PrimaryKey,
    fields []Field,
) ([]*Data, error) {
    stringFields := make([]string, len(fields))
    for i, fld := range fields {
        stringFields[i] = string(fld)
    }

    spk := make([]spanner.Key, len(primaryKeys))
    for i, pk := range primaryKeys {
        spk[i] = spanner.Key{
            {{- range .PrimaryKeys }}
            pk.{{ .CamelField }},
            {{- end }}
        }
    }

    iter := tx.Read(ctx, Table, spanner.KeySetFromKeys(spk...), stringFields)
    defer iter.Stop()

    var res []*Data
    if err := iter.Do(func(row *spanner.Row) error {
        var data Data
        if err := row.Columns(data.fieldPtrs(fields)...); err != nil {
            f.logError("GetByPrimaryKeysTx", "Failed to Scan", logger.H{
                "error":  err,
                "fields": fields,
            })
            return err
        }
        res = append(res, &data)
        return nil
    }); err != nil {
        return nil, err
    }

    return res, nil
}


func (f *Facade) ListByPrimaryKeysRtx(
	ctx context.Context,
	rtx *spanner.ReadOnlyTransaction,
	primaryKeys []PrimaryKey,
) ([]*Data, error) {
	return f.GetByPrimaryKeysRtx(ctx, rtx, primaryKeys, allFieldsList)
}

func (f *Facade) ListByPrimaryKeysTx(
	ctx context.Context,
	tx *spanner.ReadWriteTransaction,
	primaryKeys []PrimaryKey,
) ([]*Data, error) {
	return f.GetByPrimaryKeysTx(ctx, tx, primaryKeys, allFieldsList)
}

func (f *Facade) GetByPrimaryKeysIter(
	ctx context.Context,
	primaryKeys []PrimaryKey,
	fields []Field,
	callback func(*Data),
) error {
	stringFields := make([]string, len(fields))
	for i, f := range fields {
		stringFields[i] =  string(f)
	}

	spk := make([]spanner.Key, len(primaryKeys))
	for i, pk := range primaryKeys {
		spk[i] = spanner.Key{
			{{- range .PrimaryKeys }}
			pk.{{.CamelField}},
			{{- end }}
		}
	}

	if err := f.db.Single().Read(ctx, Table, spanner.KeySetFromKeys(spk...), stringFields).Do(func(row *spanner.Row) error {
		var data Data

		if err := row.Columns(data.fieldPtrs(fields)...); err != nil {
			f.logError("GetByPrimaryKeysIter", "Failed to Scan", logger.H{
				"error":        err,
				"fields":       fields,
			})
			return err
		}

		callback(&data)

		return nil
	}); err != nil {
		return err
	}

	return nil
}

func (f *Facade) ListByPrimaryKeysIter(
	ctx context.Context,
	primaryKeys []PrimaryKey,
	callback func(*Data),
) error {
	return f.GetByPrimaryKeysIter(ctx, primaryKeys, allFieldsList, callback)
}

func (f *Facade) List(
	ctx context.Context,
	queryParams []QueryParam,
) ([]*Data, error) {
	return f.Get(ctx, queryParams, allFieldsList)
}

func (f *Facade) ListRtx(
	ctx context.Context,
	rtx *spanner.ReadOnlyTransaction,
	queryParams []QueryParam,
) ([]*Data, error) {
	return f.GetRtx(ctx, rtx, queryParams, allFieldsList)
}

func (f *Facade) ListIter(
	ctx context.Context,
	queryParams []QueryParam,
	callback func(*Data),
) error {
	return f.GetIter(ctx, queryParams, allFieldsList, callback)
}

func (f *Facade) ListRtxIter(
	ctx context.Context,
	rtx *spanner.ReadOnlyTransaction,
	queryParams []QueryParam,
	callback func(*Data),
) error {
	return f.GetRtxIter(ctx, rtx, queryParams, allFieldsList, callback)
}
`

var newTemplateString = `

type readtype string

const (
	byKeys    readtype = "byKeys"
	byRange   readtype = "byRange"
	byQuery   readtype = "byQuery"
	byParams  readtype = "byParams"
	byIndex   readtype = "byIndex"
	byIndexes readtype = "byIndexes"
	byBuilder readtype = "byBuilder"
	byCounter readtype = "byCounter"
)

type OperationRead struct {
	f         *Facade
	fields    []Field
	strFields []string
	stmt      spanner.Statement
	readtype  readtype
	rtx       *spanner.ReadOnlyTransaction
	tx        *spanner.ReadWriteTransaction
	spk       []spanner.Key
	keyrange  spanner.KeyRange
	params    []interface{}
	qp        []QueryParam
	singleKey spanner.Key
	keyList   []spanner.Key
	indxName  string
	qb        *sql_builder.Builder[Field]
}

func (op *OperationRead) Exists(
    ctx context.Context, 
	{{- range .PrimaryKeys }}
		{{.Camel }} {{.Type}},
	{{- end }}
) bool {
	tx := op.rtx
	if tx == nil {
		tx = op.f.db.Single()
	}
	_, err := tx.ReadRow(
		ctx,
		Table,
		spanner.Key{
			{{- range .PrimaryKeys }}
			{{ .Camel}},
			{{- end }}
		},
		[]string{string(ID)},
	)
	return err == nil
}

func (op *OperationRead) Columns(fields ...Field) *OperationRead {
	if len(fields) == 0 {
		return op
	}
	op.fields = fields
	return op
}

func (op *OperationRead) Params(queryParams []QueryParam) *OperationRead {
	op.readtype = byParams
	op.qp = queryParams

	return op
}

func (op *OperationRead) Query(stmt spanner.Statement) *OperationRead {
	op.readtype = byQuery
	op.stmt = stmt
	return op
}

func (op *OperationRead) Rtx(rtx *spanner.ReadOnlyTransaction) *OperationRead {
	op.rtx = rtx
	return op
}

func (op *OperationRead) Tx(tx *spanner.ReadWriteTransaction) *OperationRead {
	op.tx = tx
	return op
}

func (op *OperationRead) ByKeys(primaryKeys []PrimaryKey) *OperationRead {
	spk := make([]spanner.Key, len(primaryKeys))
	for i, pk := range primaryKeys {
		spk[i] = spanner.Key{
            {{- range .PrimaryKeys }}
			pk.{{.CamelField}},
			{{- end }}
		}
	}
	op.readtype = byKeys
	op.spk = spk
	return op
}

// If you do not provide columns via Columns method, by default it will select only columns which are primary keys
func (op *OperationRead) ByIndexKeyList(indexName string, keys []spanner.Key) *OperationRead {
	op.readtype = byIndexes
	op.indxName = indexName
	op.keyList = keys
	return op
}

func convertColumns(fields []Field) []string {
	stringFields := make([]string, len(fields))
	for i, f := range fields {
		stringFields[i] = string(f)
	}
	return stringFields
}

func (op *OperationRead) stringColumns() {
	if len(op.fields) == 0 || len(op.fields) > len(allFieldsList) {
		op.fields = allFieldsList
	}
	op.strFields = convertColumns(op.fields)
}

func (op *OperationRead) primaryColumns() {
	if len(op.fields) == 0 {
		op.fields = []Field{
			{{- range .PrimaryKeys }}
				{{.CamelField}},
			{{- end }}
		}
	}
	op.strFields = convertColumns(op.fields)
}


func (op *OperationRead) Select(fields ...Field) *sql_builder.Builder[Field] {
	op.qb = sql_builder.New[Field](SelectQuery(fields))
	op.readtype = byBuilder
	op.fields = fields
	return op.qb
}

func (op *OperationRead) SelectAll() *sql_builder.Builder[Field] {
	op.qb = sql_builder.New[Field](SelectQuery(nil))
	op.readtype = byBuilder
	return op.qb
}

func (op *OperationRead) SelectCount(columns ...Field) *sql_builder.Builder[Field] {
	startQuery := strings.Builder{}
	startQuery.WriteString("SELECT COUNT(")
	if len(columns) == 0 {
		startQuery.WriteString("*")
	} else {
		startQuery.WriteString(strings.Join(makeStringFields(columns), ", "))
	}
	startQuery.WriteString(") FROM ")
	startQuery.WriteString(Table)
	op.qb = sql_builder.New[Field](startQuery.String())
	op.readtype = byCounter
	startQuery.Reset()
	return op.qb
}

func (op *OperationRead) GetCount(ctx context.Context) (int64, error) {
	if op.qb == nil {
		op.SelectCount()
	}
	if op.rtx == nil {
		op.rtx = op.f.db.Single()
	}
	iter := op.rtx.Query(ctx, spanner.Statement{
		SQL:    op.qb.String(),
		Params: op.qb.Params(),
	})
	defer iter.Stop()

	var count int64
	if err := iter.Do(func(row *spanner.Row) error {
		if err := row.Columns(&count); err != nil {
			op.f.logError("GetCount", "Failed to Scan", logger.H{
				"error": err,
				"query": op.qb.String(),
				"param": op.qb.Params(),
			})
			return err
		}
		return nil
	}); err != nil {
		return 0, err
	}

	return count, nil
}

func (op *OperationRead) Iterator(ctx context.Context) *spanner.RowIterator {
	defer op.qb.Reset()
	if op.rtx == nil {
		op.rtx = op.f.db.Single()
	}
	switch op.readtype {
	case byKeys:
		op.params = []interface{}{op.spk}
		op.stringColumns()
		return op.rtx.Read(ctx, Table, spanner.KeySetFromKeys(op.spk...), op.strFields)
	case byRange:
		op.params = []interface{}{op.keyrange}
		op.stringColumns()
		return op.rtx.Read(ctx, Table, op.keyrange, op.strFields)
	case byQuery:
		op.params = []interface{}{op.stmt}
		op.stringColumns()
		return op.rtx.Query(ctx, op.stmt)
	case byIndex:
		op.params = []interface{}{op.singleKey}
		op.stringColumns()
		return op.rtx.ReadUsingIndex(ctx, Table, op.indxName, op.singleKey, op.strFields)
	case byIndexes:
		op.params = []interface{}{op.keyList}
		op.primaryColumns()
		return op.rtx.ReadUsingIndex(ctx, Table, op.indxName, spanner.KeySetFromKeys(op.keyList...), op.strFields)
	case byBuilder:
		op.stringColumns()
		op.params = []interface{}{op.qb.Params()}
		return op.rtx.Query(ctx, spanner.Statement{
			SQL:    op.qb.String(),
			Params: op.qb.Params(),
		})
	case byParams:
		op.stringColumns()
		queryString := SelectQuery(op.fields)
		whereClauses, params := ConstructWhereClause(op.qp)
		if op.qp != nil && len(op.qp) > 0 {
			queryString += " WHERE " + whereClauses
		}
		op.params = []interface{}{op.qp}
		return op.rtx.Query(ctx, spanner.Statement{
			SQL:    queryString,
			Params: params,
		})
	case byCounter:
		panic("OperationRead Iterator: byCounter is not supported. Use GetCount instead")
	default:
		return nil
	}
}

func (op *OperationRead) DoIter(ctx context.Context, callback func(*Data)) error {
	withLog := func(row *spanner.Row) error {
		var data Data
		if err := row.Columns(data.fieldPtrs(op.fields)...); err != nil {
			op.f.logError("OperationRead DoIter", "Failed to Scan", logger.H{
				"error":        err,
				"query_params": op.params,
				"fields":       op.fields,
			})
			return err
		}

		callback(&data)

		return nil
	}

	if err := op.Iterator(ctx).Do(withLog); err != nil {
		return err
	}

	return nil
}

func (op *OperationRead) Rows(ctx context.Context) ([]*Data, error) {
	iter := op.Iterator(ctx)
	defer iter.Stop()

	res := make([]*Data, 0, iter.RowCount)

	if err := iter.Do(func(row *spanner.Row) error {
		var data Data
		if err := row.Columns(data.fieldPtrs(op.fields)...); err != nil {
			op.f.logError("OperationRead Rows", "Failed to Scan", logger.H{
				"error":        err,
				"query_params": op.params,
				"fields":       op.fields,
			})
			return err
		}

		res = append(res, &data)

		return nil
	}); err != nil {
		return nil, err
	}

	return res, nil
}

func (op *OperationRead) SingleRow(
	ctx context.Context,
	{{- range .PrimaryKeys }}
	{{.Camel }} {{.Type}},
	{{- end }}
) (*Data, error) {
	if op.rtx == nil {
		op.rtx = op.f.db.Single()
	}
	if len(op.fields) == 0 || len(op.fields) > len(allFieldsList) {
		op.fields = allFieldsList
	}
	row, err := op.rtx.ReadRow(ctx, Table,
		spanner.Key{
			{{- range .PrimaryKeys }}
			{{ .Camel }},
			{{- end }}
		},
		convertColumns(op.fields),
	)
	if err != nil {
		op.f.logError("ReadRow", "Failed to ReadRow", logger.H{
			"error":           err,
			"fields":          op.fields,
        {{- range .PrimaryKeys }}
            "{{.Snake}}": {{.Camel}},
        {{- end }}
		})
		return nil, err
	}

	var data Data
	err = row.Columns(data.fieldPtrs(op.fields)...)
	if err != nil {
		op.f.logError("ReadRow", "Failed to Scan", logger.H{
			"error":  err,
			"fields": op.fields,
		})
		return nil, err
	}

	return &data, nil
}


type Muts struct {
}

var mutations = &Muts{}

func (op *Muts) Create(data *Data) *spanner.Mutation {
	return spanner.Insert(Table, allStringFields, GetValues(data))
}

// Put is an alias for spanner.InsertOrUpdate
func (op Muts) Put(data *Data) *spanner.Mutation {
	return spanner.InsertOrUpdate(Table, allStringFields, GetValues(data))
}

func (op *Muts) Delete(
	{{- range .PrimaryKeys }}
	{{.Camel }} {{.Type}},
	{{- end }}
) *spanner.Mutation {
	return spanner.Delete(Table, spanner.Key{
		{{- range .PrimaryKeys }}
		{{.Camel}},
		{{- end }}
	})
}

func (op *Muts) Update(
	{{- range .PrimaryKeys }}
	{{.Camel }} {{.Type}},
	{{- end }}
	data UpdateFields,
) *spanner.Mutation {
	mutationData := map[string]interface{}{
		{{- range .PrimaryKeys }}
			{{.CamelField}}.String(): {{.Camel}},
		{{- end }}
	}
	for field, value := range data {
		mutationData[field.String()] = value
	}

	return spanner.UpdateMap(Table, mutationData)
}

type OperationWrite struct {
	f    *Facade
	muts []*spanner.Mutation
}

func (op *OperationWrite) apply(ctx context.Context, muts []*spanner.Mutation) error {
	if _, err := op.f.db.Apply(ctx, muts); err != nil {
		op.f.logError("OperationWrite Apply", "Failed to Apply", logger.H{
			"error": err,
			"muts":  muts,
		})
		return fmt.Errorf("failed to delete file record: %w", err)
	}

	return nil
}

func (op *OperationWrite) Update(
	{{- range .PrimaryKeys }}
	{{.Camel }} {{.Type}},
	{{- end }}
	data UpdateFields,
) *OperationWrite {
	op.muts = append(
		op.muts,
		mutations.Update(
			{{- range .PrimaryKeys }}
			{{.Camel }},
			{{- end }}
			data,
		),
	)

	return op
}

func (op *OperationWrite) Create(data *Data) *OperationWrite {
	op.muts = append(op.muts, mutations.Create(data))
	return op
}

// Put is an alias for spanner.InsertOrUpdate
func (op *OperationWrite) Put(data *Data) *OperationWrite {
	op.muts = append(op.muts, mutations.Put(data))
	return op
}

func (op *OperationWrite) Apply(ctx context.Context) error {
	return op.apply(ctx, op.muts)
}

func (op *OperationWrite) Delete(
	{{- range .PrimaryKeys }}
	{{.Camel }} {{.Type}},
	{{- end }}
) *OperationWrite {
	op.muts = append(op.muts, mutations.Delete(
		{{- range .PrimaryKeys }}
		{{.Camel}},
		{{- end }}
	))

	return op
}

func (op *OperationWrite) GetMuts() []*spanner.Mutation {
	return op.muts
}

func Mut() *Muts {
	return mutations
}

func (f *Facade) Read() *OperationRead {
	return &OperationRead{
		f		: f,
		readtype: byParams,
	}
}

func (f *Facade) Write() *OperationWrite {
	return &OperationWrite{
		f: f,
	}
}

func (f *Facade) Muts() *Muts {
	return mutations
}
`

var newTemplateStringMoreThanOnePKAddOn = `

func (op *OperationRead) By{{- $first := index .PrimaryKeys 0 }}{{- $first.CamelField }}(
    {{ $first.Camel }} {{ $first.Type }},
) *OperationRead {
    op.keyrange = spanner.Key{ {{ $first.Camel }} }.AsPrefix()
	op.readtype = byRange

	return op
}
`

var oldTemplateStringMoreThanOnePKAddOn = `

func (f *Facade) GetBy{{- $first := index .PrimaryKeys 0 }}{{- $first.CamelField }}(
	ctx context.Context,
	{{ $first.Camel }} {{ $first.Type }},
	fields []Field,
) ([]*Data, error) {
	stringFields := make([]string, len(fields))
	for i, f := range fields {
		stringFields[i] = string(f)
	}

	iter := f.db.Single().Read(
		ctx,
		Table,
		spanner.Key{ {{ $first.Camel }} }.AsPrefix(),
		stringFields,
	)
	defer iter.Stop()

	var res []*Data

	if err := iter.Do(func(row *spanner.Row) error {
		var data Data

		if err := row.Columns(data.fieldPtrs(fields)...); err != nil {
			f.logError("GetBy{{ $first.Camel }}", "Failed to Scan", logger.H{
				"error":        err,
				"{{ $first.Snake }}": {{ $first.Camel }},
				"fields":       fields,
			})
			return err
		}

		res = append(res, &data)

		return nil
	}); err != nil {
		return nil, err
	}

	return res, nil
}

func (f *Facade) ListBy{{- $first := index .PrimaryKeys 0 }}{{- $first.CamelField }}(
	ctx context.Context,
	{{ $first.Camel }} {{ $first.Type }},
) ([]*Data, error) {
	return f.GetBy{{ $first.CamelField }}(ctx, {{ $first.Camel }}, allFieldsList)
}

func (f *Facade) GetBy{{- $first := index .PrimaryKeys 0 }}{{- $first.CamelField }}Rtx(
	ctx context.Context,
	rtx *spanner.ReadOnlyTransaction,
	{{ $first.Camel }} {{ $first.Type }},
	fields []Field,
) ([]*Data, error) {
	stringFields := make([]string, len(fields))
	for i, f := range fields {
		stringFields[i] = string(f)
	}

	iter := rtx.Read(
		ctx,
		Table,
		spanner.Key{ {{ $first.Camel }} }.AsPrefix(),
		stringFields,
	)
	defer iter.Stop()

	var res []*Data

	if err := iter.Do(func(row *spanner.Row) error {
		var data Data

		if err := row.Columns(data.fieldPtrs(fields)...); err != nil {
			f.logError("GetBy{{ $first.Camel }}", "Failed to Scan", logger.H{
				"error":        err,
				"{{ $first.Snake }}": {{ $first.Camel }},
				"fields":       fields,
			})
			return err
		}

		res = append(res, &data)

		return nil
	}); err != nil {
		return nil, err
	}

	return res, nil
}

func (f *Facade) ListBy{{- $first := index .PrimaryKeys 0 }}{{- $first.CamelField }}Rtx(
	ctx context.Context,
	rtx *spanner.ReadOnlyTransaction,
	{{ $first.Camel }} {{ $first.Type }},
) ([]*Data, error) {
	return f.GetBy{{ $first.CamelField }}Rtx(ctx, rtx, {{ $first.Camel }}, allFieldsList)
}

func (f *Facade) GetBy{{- $first := index .PrimaryKeys 0 }}{{- $first.CamelField }}Tx(
	ctx context.Context,
	tx *spanner.ReadWriteTransaction,
	{{ $first.Camel }} {{ $first.Type }},
	fields []Field,
) ([]*Data, error) {
	stringFields := make([]string, len(fields))
	for i, f := range fields {
		stringFields[i] = string(f)
	}

	iter := tx.Read(
		ctx,
		Table,
		spanner.Key{ {{ $first.Camel }} }.AsPrefix(),
		stringFields,
	)
	defer iter.Stop()

	var res []*Data

	if err := iter.Do(func(row *spanner.Row) error {
		var data Data

		if err := row.Columns(data.fieldPtrs(fields)...); err != nil {
			f.logError("GetBy{{ $first.Camel }}", "Failed to Scan", logger.H{
				"error":        err,
				"{{ $first.Snake }}": {{ $first.Camel }},
				"fields":       fields,
			})
			return err
		}

		res = append(res, &data)

		return nil
	}); err != nil {
		return nil, err
	}

	return res, nil
}

func (f *Facade) ListBy{{- $first := index .PrimaryKeys 0 }}{{- $first.CamelField }}Tx(
	ctx context.Context,
	tx *spanner.ReadWriteTransaction,
	{{ $first.Camel }} {{ $first.Type }},
) ([]*Data, error) {
	return f.GetBy{{ $first.CamelField }}Tx(ctx, tx, {{ $first.Camel }}, allFieldsList)
}

func (f *Facade) GetBy{{- $first := index .PrimaryKeys 0 }}{{- $first.CamelField }}Iter(
	ctx context.Context,
	{{ $first.Camel }} {{ $first.Type }},
	fields []Field,
	callback func(*Data),
) error {
	stringFields := make([]string, len(fields))
	for i, f := range fields {
		stringFields[i] = string(f)
	}

	if err := f.db.Single().Read(
		ctx,
		Table,
		spanner.Key{ {{ $first.Camel }} }.AsPrefix(),
		stringFields,
	).Do(func(row *spanner.Row) error {
		var data Data

		if err := row.Columns(data.fieldPtrs(fields)...); err != nil {
			f.logError("GetBy{{ $first.Camel }}Iter", "Failed to Scan", logger.H{
				"error":        err,
				"{{ $first.Snake }}": {{ $first.Camel }},
				"fields":       fields,
			})
			return err
		}

		callback(&data)

		return nil
	}); err != nil {
		return err
	}

	return nil
}

func (f *Facade) ListBy{{- $first := index .PrimaryKeys 0 }}{{- $first.CamelField }}Iter(
	ctx context.Context,
	{{ $first.Camel }} {{ $first.Type }},
	callback func(*Data),
) error {
	return f.GetBy{{ $first.CamelField }}Iter(ctx, {{ $first.Camel }}, allFieldsList, callback)
}

func (f *Facade) GetBy{{- $first := index .PrimaryKeys 0 }}{{- $first.CamelField }}RtxIter(
	ctx context.Context,
	rtx *spanner.ReadOnlyTransaction,
	{{ $first.Camel }} {{ $first.Type }},
	fields []Field,
	callback func(*Data),
) error {
	stringFields := make([]string, len(fields))
	for i, f := range fields {
		stringFields[i] = string(f)
	}

	if err := rtx.Read(
		ctx,
		Table,
		spanner.Key{ {{ $first.Camel }} }.AsPrefix(),
		stringFields,
	).Do(func(row *spanner.Row) error {
		var data Data

		if err := row.Columns(data.fieldPtrs(fields)...); err != nil {
			f.logError("GetBy{{ $first.Camel }}Iter", "Failed to Scan", logger.H{
				"error":        err,
				"{{ $first.Snake }}": {{ $first.Camel }},
				"fields":       fields,
			})
			return err
		}

		callback(&data)

		return nil
	}); err != nil {
		return err
	}

	return nil
}

func (f *Facade) ListBy{{- $first := index .PrimaryKeys 0 }}{{- $first.CamelField }}RtxIter(
	ctx context.Context,
	rtx *spanner.ReadOnlyTransaction,
	{{ $first.Camel }} {{ $first.Type }},
	callback func(*Data),
) error {
	return f.GetBy{{ $first.CamelField }}RtxIter(ctx, rtx, {{ $first.Camel }}, allFieldsList, callback)
}

func (f *Facade) GetBy{{- $first := index .PrimaryKeys 0 }}{{- $first.CamelField }}TxIter(
	ctx context.Context,
	tx *spanner.ReadWriteTransaction,
	{{ $first.Camel }} {{ $first.Type }},
	fields []Field,
	callback func(*Data),
) error {
	stringFields := make([]string, len(fields))
	for i, f := range fields {
		stringFields[i] = string(f)
	}

	if err := tx.Read(
		ctx,
		Table,
		spanner.Key{ {{ $first.Camel }} }.AsPrefix(),
		stringFields,
	).Do(func(row *spanner.Row) error {
		var data Data

		if err := row.Columns(data.fieldPtrs(fields)...); err != nil {
			f.logError("GetBy{{ $first.Camel }}Iter", "Failed to Scan", logger.H{
				"error":        err,
				"{{ $first.Snake }}": {{ $first.Camel }},
				"fields":       fields,
			})
			return err
		}

		callback(&data)

		return nil
	}); err != nil {
		return err
	}

	return nil
}

func (f *Facade) ListBy{{- $first := index .PrimaryKeys 0 }}{{- $first.CamelField }}TxIter(
	ctx context.Context,
	tx *spanner.ReadWriteTransaction,
	{{ $first.Camel }} {{ $first.Type }},
	callback func(*Data),
) error {
	return f.GetBy{{ $first.CamelField }}TxIter(ctx, tx, {{ $first.Camel }}, allFieldsList, callback)
}

func (f *Facade) DeleteBy{{- $first := index .PrimaryKeys 0 }}{{- $first.CamelField }}Mut(
	{{ $first.Camel }} {{ $first.Type }},
) *spanner.Mutation {
	return spanner.Delete(Table, spanner.Key{ {{- $first.Camel }} }.AsPrefix())
}

func (f *Facade) DeleteBy{{- $first := index .PrimaryKeys 0 }}{{- $first.CamelField }}(
	ctx context.Context,
	{{ $first.Camel }} {{ $first.Type }},
) error {
	mutation := f.DeleteBy{{ $first.CamelField }}Mut({{ $first.Camel }})

	if _, err := f.db.Apply(ctx, []*spanner.Mutation{mutation}); err != nil {
		f.logError("DeleteBy{{ $first.Camel }}", "Failed to Apply", logger.H{
			"error": err,
			"{{ $first.Snake }}": {{ $first.Camel }},
		})
		return fmt.Errorf("failed to delete file record: %w", err)
	}

	return nil
}
	
`

var newTemplateStringMoreThanTwoPKAddOn = `

func (op *OperationRead) By{{- $first := index .PrimaryKeys 0 }}{{- $second := index .PrimaryKeys 1 }}{{ $second.CamelField }}(
	{{ $first.Camel }} {{ $first.Type }},
	{{ $second.Camel }} {{ $second.Type }},
) *OperationRead {
    op.keyrange = spanner.Key{ {{ $first.Camel }}, {{ $second.Camel }} }.AsPrefix()
	op.readtype = byRange
	
	return op
}
`

var oldTemplateStringMoreThanTwoPKAddOn = `

func (f *Facade) GetBy{{- $first := index .PrimaryKeys 0 }}{{- $second := index .PrimaryKeys 1 }}{{ $second.CamelField }}(
	ctx context.Context,
	{{ $first.Camel }} {{ $first.Type }},
	{{ $second.Camel }} {{ $second.Type }},
	fields []Field,
) ([]*Data, error) {
	stringFields := make([]string, len(fields))
	for i, f := range fields {
		stringFields[i] = string(f)
	}

	var res []*Data
	if err := f.db.Single().Read(
		ctx,
		Table,
		spanner.Key{ {{ $first.Camel }}, {{ $second.Camel }} }.AsPrefix(),
		stringFields,
	).Do(func(row *spanner.Row) error {
		var data Data
		if err := row.Columns(data.fieldPtrs(fields)...); err != nil {
			f.logError("GetBy{{ $first.Camel }}And{{ $second.Camel }}", "Failed to Scan", logger.H{
				"error":  err,
				"{{- $first.Snake }}": {{ $first.Camel }},
				"{{- $second.Snake }}": {{ $second.Camel }},
				"fields": fields,
			})
			return err
		}

		res = append(res, &data)
		return nil
	}); err != nil {
		return nil, err
	}

	return res, nil
}

func (f *Facade) ListBy{{- $first := index .PrimaryKeys 0 }}{{- $second := index .PrimaryKeys 1 }}{{ $second.CamelField }}(
	ctx context.Context,
	{{ $first.Camel }} {{ $first.Type }},
	{{ $second.Camel }} {{ $second.Type }},
) ([]*Data, error) {
	return f.GetBy{{ $second.CamelField }}(ctx, {{ $first.Camel }}, {{ $second.Camel }}, allFieldsList)
}

func (f *Facade) GetBy{{- $first := index .PrimaryKeys 0 }}{{- $second := index .PrimaryKeys 1 }}{{ $second.CamelField }}Rtx(
	ctx context.Context,
	rtx *spanner.ReadOnlyTransaction,
	{{ $first.Camel }} {{ $first.Type }},
	{{ $second.Camel }} {{ $second.Type }},
	fields []Field,
) ([]*Data, error) {
	stringFields := make([]string, len(fields))
	for i, f := range fields {
		stringFields[i] = string(f)
	}
		
	var res []*Data
	if err := rtx.Read(
		ctx,
		Table,
		spanner.Key{ {{ $first.Camel }}, {{ $second.Camel }} }.AsPrefix(),
		stringFields,
	).Do(func(row *spanner.Row) error {
			var data Data
			if err := row.Columns(data.fieldPtrs(fields)...); err != nil {
				f.logError("GetBy{{ $first.Camel }}And{{ $second.Camel }}", "Failed to Scan", logger.H{
					"error":  err,
					"{{- $first.Snake }}": {{ $first.Camel }},
					"{{- $second.Snake }}": {{ $second.Camel }},
					"fields": fields,
				})
				return err
			}
				res = append(res, &data)
				return nil
	}); err != nil {
		return nil, err
	}

	return res, nil
}

func (f *Facade) GetBy{{- $first := index .PrimaryKeys 0 }}{{- $second := index .PrimaryKeys 1 }}{{ $second.CamelField }}Tx(
	ctx context.Context,
	tx *spanner.ReadWriteTransaction,
	{{ $first.Camel }} {{ $first.Type }},
	{{ $second.Camel }} {{ $second.Type }},
	fields []Field,
) ([]*Data, error) {
	stringFields := make([]string, len(fields))
	for i, f := range fields {
		stringFields[i] = string(f)
	}
		
	var res []*Data
	if err := tx.Read(
		ctx,
		Table,
		spanner.Key{ {{ $first.Camel }}, {{ $second.Camel }} }.AsPrefix(),
		stringFields,
	).Do(func(row *spanner.Row) error {
			var data Data
			if err := row.Columns(data.fieldPtrs(fields)...); err != nil {
				f.logError("GetBy{{ $first.Camel }}And{{ $second.Camel }}", "Failed to Scan", logger.H{
					"error":  err,
					"{{- $first.Snake }}": {{ $first.Camel }},
					"{{- $second.Snake }}": {{ $second.Camel }},
					"fields": fields,
				})
				return err
			}
				res = append(res, &data)
				return nil
	}); err != nil {
		return nil, err
	}

	return res, nil
}

func (f *Facade) ListBy{{- $first := index .PrimaryKeys 0 }}{{- $second := index .PrimaryKeys 1 }}{{ $second.CamelField }}Rtx(
	ctx context.Context,
	rtx *spanner.ReadOnlyTransaction,
	{{ $first.Camel }} {{ $first.Type }},
	{{ $second.Camel }} {{ $second.Type }},
) ([]*Data, error) {
	return f.GetBy{{ $second.CamelField }}Rtx(ctx, rtx, {{ $first.Camel }}, {{ $second.Camel }}, allFieldsList)
}

func (f *Facade) ListBy{{- $first := index .PrimaryKeys 0 }}{{- $second := index .PrimaryKeys 1 }}{{ $second.CamelField }}Tx(
	ctx context.Context,
	tx *spanner.ReadWriteTransaction,
	{{ $first.Camel }} {{ $first.Type }},
	{{ $second.Camel }} {{ $second.Type }},
) ([]*Data, error) {
	return f.GetBy{{ $second.CamelField }}Tx(ctx, tx, {{ $first.Camel }}, {{ $second.Camel }}, allFieldsList)
}

func (f *Facade) GetBy{{- $first := index .PrimaryKeys 0 }}{{- $second := index .PrimaryKeys 1 }}{{ $second.CamelField }}Iter(
	ctx context.Context,
	{{ $first.Camel }} {{ $first.Type }},
	{{ $second.Camel }} {{ $second.Type }},
	fields []Field,
	callback func(*Data),
) error {
	stringFields := make([]string, len(fields))
	for i, f := range fields {
		stringFields[i] = string(f)
	}

	if err := f.db.Single().Read(
		ctx,
		Table,
		spanner.Key{ {{ $first.Camel }}, {{ $second.Camel }} }.AsPrefix(),
		stringFields,
	).Do(func(row *spanner.Row) error {
		var data Data

		if err := row.Columns(data.fieldPtrs(fields)...); err != nil {
			f.logError("GetBy{{ $first.Camel }}And{{ $second.Camel }}Iter", "Failed to Scan", logger.H{
				"error":        err,
				"{{- $first.Snake }}": {{ $first.Camel }},
				"{{- $second.Snake }}": {{ $second.Camel }},
				"fields":       fields,
			})
			return err
		}

		callback(&data)

		return nil
	}); err != nil {
		return err
	}

	return nil
}

func (f *Facade) ListBy{{- $first := index .PrimaryKeys 0 }}{{- $second := index .PrimaryKeys 1 }}{{ $second.CamelField }}Iter(
	ctx context.Context,
	{{ $first.Camel }} {{ $first.Type }},
	{{ $second.Camel }} {{ $second.Type }},
	callback func(*Data),
) error {
	return f.GetBy{{ $second.CamelField }}Iter(ctx, {{ $first.Camel }}, {{ $second.Camel }}, allFieldsList, callback)
}

func (f *Facade) GetBy{{- $first := index .PrimaryKeys 0 }}{{- $second := index .PrimaryKeys 1 }}{{ $second.CamelField }}RtxIter(
	ctx context.Context,
	rtx *spanner.ReadOnlyTransaction,
	{{ $first.Camel }} {{ $first.Type }},
	{{ $second.Camel }} {{ $second.Type }},
	fields []Field,
	callback func(*Data),
) error {
	stringFields := make([]string, len(fields))
	for i, f := range fields {
		stringFields[i] = string(f)
	}

	if err := rtx.Read(
		ctx,
		Table,
		spanner.Key{ {{ $first.Camel }}, {{ $second.Camel }} }.AsPrefix(),
		stringFields,
	).Do(func(row *spanner.Row) error {
		var data Data

		if err := row.Columns(data.fieldPtrs(fields)...); err != nil {
			f.logError("GetBy{{ $first.Camel }}And{{ $second.Camel }}Iter", "Failed to Scan", logger.H{
				"error":        err,
				"{{- $first.Snake }}": {{ $first.Camel }},
				"{{- $second.Snake }}": {{ $second.Camel }},
				"fields":       fields,
			})
			return err
		}

		callback(&data)

		return nil
	}); err != nil {
		return err
	}

	return nil
}

func (f *Facade) ListBy{{- $first := index .PrimaryKeys 0 }}{{- $second := index .PrimaryKeys 1 }}{{ $second.CamelField }}RtxIter(
	ctx context.Context,
	rtx *spanner.ReadOnlyTransaction,
	{{ $first.Camel }} {{ $first.Type }},
	{{ $second.Camel }} {{ $second.Type }},
	callback func(*Data),
) error {
	return f.GetBy{{ $second.CamelField }}RtxIter(ctx, rtx, {{ $first.Camel }}, {{ $second.Camel }}, allFieldsList, callback)
}

func (f *Facade) DeleteBy{{- $first := index .PrimaryKeys 0 }}{{- $second := index .PrimaryKeys 1 }}{{ $second.CamelField }}Mut(
	{{ $first.Camel }} {{ $first.Type }},
	{{ $second.Camel }} {{ $second.Type }},
) *spanner.Mutation {
	return spanner.Delete(Table, spanner.Key{ {{- $first.Camel }}, {{- $second.Camel }} }.AsPrefix())
}

func (f *Facade) DeleteBy{{- $first := index .PrimaryKeys 0 }}{{- $second := index .PrimaryKeys 1 }}{{ $second.CamelField }}(
	ctx context.Context,
	{{ $first.Camel }} {{ $first.Type }},
	{{ $second.Camel }} {{ $second.Type }}, 
) error {
	mutation := f.DeleteBy{{ $second.CamelField }}Mut({{ $first.Camel }}, {{ $second.Camel }})

	if _, err := f.db.Apply(ctx, []*spanner.Mutation{mutation}); err != nil {
		f.logError("DeleteBy{{ $first.Camel }}And{{ $second.Camel }}", "Failed to Apply", logger.H{
			"error": err,
			"{{- $first.Snake }}": {{ $first.Camel }},
			"{{- $second.Snake }}": {{ $second.Camel }},
		})
		return fmt.Errorf("failed to delete file record: %w", err)
	}

	return nil
}
`

var newTemplateStringSecondaryIndexAddOn = `
{{- range .SecondatyIndexes }}
		func (op *OperationRead) defaultFields{{.IndexName.CamelField}}() {
			if len(op.fields) == 0 || len(op.fields) > len(allFieldsList) {
				op.fields = []Field{
					{{- range .PrimaryKeys }}
					{{.CamelField}},
					{{- end }}
					{{- range .Fields }}
					{{.CamelField}},
					{{- end }}
				}
			}
		}

		// If you do not provide columns via Columns method, by default it will select only columns which are primary
		func (op *OperationRead) By{{.IndexName.CamelField}}(
			{{- range .Fields }}
			{{.Camel}} {{.Type}},
			{{- end }}
		) *OperationRead {
			op.singleKey = spanner.Key{
				{{- range .Fields }}
				{{.Camel}},
				{{- end }}
			}
			op.readtype = byIndex
			op.indxName = {{.IndexName.CamelField}}
			op.defaultFields{{.IndexName.CamelField}}()

			return op
		}

		// If you do not provide columns via Columns method, by default it will select only columns which are primary and index
		func (op *OperationRead) SingleBy{{.IndexName.CamelField}}(
			ctx context.Context,
			{{- range .Fields }}
			{{.Camel}} {{.Type}},
			{{- end }}
		) (*Data, error) {
			if op.rtx == nil {
				op.rtx = op.f.db.Single()
			}
			op.defaultFields{{.IndexName.CamelField}}()

			row, err := op.rtx.ReadRowUsingIndex(
				ctx,
				Table,
				{{.IndexName.CamelField}},
				spanner.Key{
					{{- range .Fields }}
					{{ .Camel }},
					{{- end }}
				},
				convertColumns(op.fields),
			)
			if err != nil {
				op.f.logError("ReadRowUsingIndex", "Failed to ReadRow", logger.H{
					"error":           err,
					"fields":          op.fields,
				{{- range .Fields }}
					"{{.Snake}}": {{.Camel}},
				{{- end }}
				})
				return nil, err
			}

			var data Data
			err = row.Columns(data.fieldPtrs(op.fields)...)
			if err != nil {
				op.f.logError("ReadRowUsingIndex", "Failed to Scan", logger.H{
					"error":  err,
					"fields": op.fields,
				{{- range .Fields }}
					"{{.Snake}}": {{.Camel}},
				{{- end }}
				})
				return nil, err
			}

			return &data, nil
		}
{{- end }}
`

var oldTemplateStringSecondaryIndexAddOn = `

	{{- range .SecondatyIndexes }}
		func (f *Facade) GetBy{{.IndexName.CamelField}}(
			ctx context.Context,
			{{- range .Fields }}
			{{.Camel}} {{.Type}},
			{{- end }}
			fields []Field,
		) ([]*Data, error) {
			stringFields := make([]string, len(fields))
			for i, f := range fields {
				stringFields[i] = string(f)
			}

			iter := f.db.Single().ReadUsingIndex(
				ctx,
				Table,
				{{.IndexName.CamelField}},
				spanner.Key{
					{{- range .Fields }}
					{{.Camel}},
					{{- end }}
				},
				stringFields,
			)
			defer iter.Stop()

			var res []*Data

			if err := iter.Do(func(row *spanner.Row) error {
				var data Data

				if err := row.Columns(data.fieldPtrs(fields)...); err != nil {
					f.logError("GetBy{{.IndexName.CamelField}}", "Failed to Scan", logger.H{
						"error":  err,
						"fields": fields,
					})
					return err
				}

				res = append(res, &data)

				return nil
			}); err != nil {
				return nil, err
			}

			return res, nil
		}

		func (f *Facade) ListBy{{.IndexName.CamelField}}(
			ctx context.Context,
			{{- range .Fields }}
			{{.Camel}} {{.Type}},
			{{- end }}
		) ([]*Data, error) {
			return f.GetBy{{.IndexName.CamelField}}(ctx, {{- range .Fields }}{{.Camel}}, {{- end }}allFieldsList)
		}

		func (f *Facade) GetBy{{.IndexName.CamelField}}Rtx(
			ctx context.Context,
			rtx *spanner.ReadOnlyTransaction,
			{{- range .Fields }}
			{{.Camel}} {{.Type}},
			{{- end }}
			fields []Field,
		) ([]*Data, error) {
			stringFields := make([]string, len(fields))
			for i, f := range fields {
				stringFields[i] = string(f)
			}
				
			var res []*Data
			iter := rtx.ReadUsingIndex(
				ctx,
				Table,
				{{.IndexName.CamelField}},
				spanner.Key{
					{{- range .Fields }}
					{{.Camel}},
					{{- end }}
				},
				stringFields,
			)
			defer iter.Stop()	
			if err := iter.Do(func(row *spanner.Row) error {
					var data Data
					if err := row.Columns(data.fieldPtrs(fields)...); err != nil {
						f.logError("GetBy{{.IndexName.CamelField}}", "Failed to Scan", logger.H{
							"error":  err,
							"fields": fields,
						})
						return err
					}
						res = append(res, &data)
						return nil
			}); err != nil {
				return nil, err
			}

			return res, nil
		}

		func (f *Facade) GetBy{{.IndexName.CamelField}}Tx(
			ctx context.Context,
			tx *spanner.ReadWriteTransaction,
			{{- range .Fields }}
			{{.Camel}} {{.Type}},
			{{- end }}
			fields []Field,
		) ([]*Data, error) {
			stringFields := make([]string, len(fields))
			for i, f := range fields {
				stringFields[i] = string(f)
			}
				
			var res []*Data
			iter := tx.ReadUsingIndex(
				ctx,
				Table,
				{{.IndexName.CamelField}},
				spanner.Key{
					{{- range .Fields }}
					{{.Camel}},
					{{- end }}
				},
				stringFields,
			)
			defer iter.Stop()	
			if err := iter.Do(func(row *spanner.Row) error {
					var data Data
					if err := row.Columns(data.fieldPtrs(fields)...); err != nil {
						f.logError("GetBy{{.IndexName.CamelField}}", "Failed to Scan", logger.H{
							"error":  err,
							"fields": fields,
						})
						return err
					}
						res = append(res, &data)
						return nil
			}); err != nil {
				return nil, err
			}

			return res, nil
		}

		func (f *Facade) ListBy{{.IndexName.CamelField}}Rtx(
			ctx context.Context,
			rtx *spanner.ReadOnlyTransaction,
			{{- range .Fields }}
			{{.Camel}} {{.Type}},
			{{- end }}
		) ([]*Data, error) {
			return f.GetBy{{.IndexName.CamelField}}Rtx(ctx, rtx, {{- range .Fields }}{{.Camel}}, {{- end }}allFieldsList)
		}

		func (f *Facade) ListBy{{.IndexName.CamelField}}Tx(
			ctx context.Context,
			tx *spanner.ReadWriteTransaction,
			{{- range .Fields }}
			{{.Camel}} {{.Type}},
			{{- end }}
		) ([]*Data, error) {
			return f.GetBy{{.IndexName.CamelField}}Tx(ctx, tx, {{- range .Fields }}{{.Camel}}, {{- end }}allFieldsList)
		}

		func (f *Facade) GetBy{{.IndexName.CamelField}}Iter(
			ctx context.Context,
			{{- range .Fields }}
			{{.Camel}} {{.Type}},
			{{- end }}
			fields []Field,
			callback func(*Data),
		) error {
			stringFields := make([]string, len(fields))
			for i, f := range fields {
				stringFields[i] = string(f)
			}

			 iter := f.db.Single().ReadUsingIndex(
				ctx,
				Table,
				{{.IndexName.CamelField}},
				spanner.Key{
					{{- range .Fields }}
					{{.Camel}},
					{{- end }}
				},
				stringFields,
			)
			defer iter.Stop()
			if err := iter.Do(func(row *spanner.Row) error {
				var data Data

				if err := row.Columns(data.fieldPtrs(fields)...); err != nil {
					f.logError("GetBy{{.IndexName.CamelField}}Iter", "Failed to Scan", logger.H{
						"error":  err,
						"fields": fields,
					})
					return err
				}

				callback(&data)

				return nil
			}); err != nil {
				return err
			}

			return nil
		}

		func (f *Facade) ListBy{{.IndexName.CamelField}}Iter(
			ctx context.Context,
			{{- range .Fields }}
			{{.Camel}} {{.Type}},
			{{- end }}
			callback func(*Data),
		) error {
			return f.GetBy{{.IndexName.CamelField}}Iter(ctx, {{- range .Fields }}{{.Camel}}, {{- end }}allFieldsList, callback)
		}

		func (f *Facade) GetBy{{.IndexName.CamelField}}RtxIter(
			ctx context.Context,
			rtx *spanner.ReadOnlyTransaction,
			{{- range .Fields }}
			{{.Camel}} {{.Type}},
			{{- end }}
			fields []Field,
			callback func(*Data),
		) error {
			stringFields := make([]string, len(fields))
			for i, f := range fields {
				stringFields[i] = string(f)
			}

			 iter := rtx.ReadUsingIndex(
				ctx,
				Table,
				{{.IndexName.CamelField}},
				spanner.Key{
					{{- range .Fields }}
					{{.Camel}},
					{{- end }}
				},
				stringFields,
			)
			defer iter.Stop()
			if err := iter.Do(func(row *spanner.Row) error {
				var data Data

				if err := row.Columns(data.fieldPtrs(fields)...); err != nil {
					f.logError("GetBy{{.IndexName.CamelField}}Iter", "Failed to Scan", logger.H{
						"error":  err,
						"fields": fields,
					})
					return err
				}

				callback(&data)

				return nil
			}); err != nil {
				return err
			}

			return nil
		}

		func (f *Facade) ListBy{{.IndexName.CamelField}}RtxIter(
			ctx context.Context,
			rtx *spanner.ReadOnlyTransaction,
			{{- range .Fields }}
			{{.Camel}} {{.Type}},
			{{- end }}
			callback func(*Data),
		) error {
			return f.GetBy{{.IndexName.CamelField}}RtxIter(ctx, rtx, {{- range .Fields }}{{.Camel}}, {{- end }}allFieldsList, callback)
		}

	{{- end }}

	
`

var templateModelString = `package {{.PackageName}}

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	"{{.ProjectName}}/smartlg/logger"
	"{{.ModuleName}}/m_options"
)


type Model struct {
	DB *pgxpool.Pool
	//
	{{- range .Tables }}
	{{ .TableName }} *{{ .PackageName }}.Facade
	{{- end }}
}

type Options struct {
	PostgresURL string
	Log *logger.Logger
}

func New(ctx context.Context, o *Options) (*Model, error) {
	// Parse config
	config, err := pgxpool.ParseConfig(o.PostgresURL)
	if err != nil {
		o.Log.Error("Failed to parse PostgreSQL config", logger.H{"error": err})
		return nil, fmt.Errorf("failed to parse PostgreSQL config: %w", err)
	}

	// Set connection pool settings
	config.MinConns = 10
	config.MaxConns = 100

	// Create connection pool
	db, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		o.Log.Error("Failed to create PostgreSQL connection pool", logger.H{"error": err})
		return nil, fmt.Errorf("failed to create PostgreSQL connection pool: %w", err)
	}

	// Test connection
	if err := ping(ctx, db); err != nil {
		o.Log.Error("[PKG DB] Failed to ping PostgreSQL.", map[string]any{
			"error": err,
		})
		return nil, fmt.Errorf("failed to ping PostgreSQL: %w", err)
	}

	opt := &m_options.Options{
		Log: o.Log,
		DB:  db,
	}

	return &Model{
		DB: db,
		//
		{{- range .Tables}}
		{{.TableName}}: {{.PackageName}}.New(opt),
		{{- end}}
	}, nil
}

func ping(ctx context.Context, db *pgxpool.Pool) error {
	var testResult int
	err := db.QueryRow(ctx, "SELECT 1").Scan(&testResult)
	if err != nil {
		return fmt.Errorf("failed to ping PostgreSQL: %w", err)
	}

	if testResult != 1 {
		return fmt.Errorf("unexpected ping result: %d", testResult)
	}

	return nil
}

func (m *Model) Close() {
	if m.DB != nil {
		m.DB.Close()
	}
}
`
var templateOptionsString = `package m_options

import (
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"{{.ProjectName}}/log/logger"
)

type Options struct {
	Log *logger.Logger
	DB  *pgxpool.Pool
}

func (o Options) IsValid() error {
	if o == (Options{}) {
		return fmt.Errorf("options is empty")
	}
	if o.Log == nil {
		return fmt.Errorf("log is nil")
	}
	if o.DB == nil {
		return fmt.Errorf("db is nil")
	}
	return nil
}`

var templateQueryBuilderString = `package sql_builder

import (
	"strconv"
	"strings"
)

// Builder is the main struct for constructing SQL queries.
// All methods for building clauses are directly on this struct.
type Builder[FieldType ~string] struct {
	params      map[string]any
	paramWriter *strings.Builder

	fields []FieldType // fields holds the selected columns for the query.

	// String builders for each clause directly in the main Builder
	selectClause  *strings.Builder
	fromClause    *strings.Builder
	whereClause   *strings.Builder
	orderByClause *strings.Builder
	groupByClause *strings.Builder
	limitClause   *strings.Builder
	offsetClause  *strings.Builder

	// afterWhere is a struct that can be used to chain methods after a WHERE clause.
	// It is not used in the Builder methods but can be useful for extending functionality.
	afterWhere *AfterWhere[FieldType]
}

type AfterWhere[FieldType ~string] struct {
	*Builder[FieldType]
}

// New creates a new instance of the Builder.
// The 'initial' string parameter is currently unused.
func New[FieldType ~string](initial string) *Builder[FieldType] {
	sqlb := &Builder[FieldType]{
		params:      make(map[string]any),
		paramWriter: &strings.Builder{},

		selectClause:  &strings.Builder{},
		fromClause:    &strings.Builder{},
		whereClause:   &strings.Builder{},
		orderByClause: &strings.Builder{},
		groupByClause: &strings.Builder{},
		limitClause:   &strings.Builder{},
		offsetClause:  &strings.Builder{},
	}
	aw := &AfterWhere[FieldType]{Builder: sqlb}
	sqlb.afterWhere = aw // Initialize AfterWhere with the current Builder
	return sqlb
}
	
// writeColumnTo is an internal helper to write a column name (quoted) to a string builder.
func (b *Builder[FieldType]) writeColumnTo(sb *strings.Builder, col FieldType) {
	sb.WriteString("` + "`" + `")
	sb.WriteString(string(col))
	sb.WriteString("` + "`" + `")
}

// addParam is an internal helper to add a parameter to the query and return its placeholder name.
func (b *Builder[FieldType]) addParam(value any) string {
	b.paramWriter.WriteString("param")
	b.paramWriter.WriteString(strconv.Itoa(len(b.params)))
	paramKey := b.paramWriter.String()
	b.paramWriter.Reset() // Reset for next param name generation

	b.paramWriter.WriteString("@")
	b.paramWriter.WriteString(paramKey)
	paramName := b.paramWriter.String()
	b.paramWriter.Reset() // Reset after generating full placeholder

	b.params[paramKey] = value
	return paramName
}

// Select starts or replaces the SELECT clause.
func (b *Builder[FieldType]) Select(columns ...FieldType) *Builder[FieldType] {
	sb := b.selectClause
	sb.Reset() // Reset the select clause for a new SELECT statement
	sb.WriteString(" SELECT ")
	for i, col := range columns {
		if i > 0 {
			sb.WriteString(", ")
		}
		b.writeColumnTo(sb, col)
	}
	b.fields = columns // Store the selected fields for potential future use
	return b
}

// From starts or replaces the FROM clause.
func (b *Builder[FieldType]) From(table string, as ...string) *Builder[FieldType] {
	fbStrBldr := b.fromClause
	fbStrBldr.Reset() // Reset the from clause
	fbStrBldr.WriteString(" FROM ")
	fbStrBldr.WriteString(table)
	if len(as) == 1 {
		fbStrBldr.WriteString(" ")
		fbStrBldr.WriteString(as[0])
	}
	return b
}

// Join adds a JOIN clause to the FROM clause.
func (b *Builder[FieldType]) Join(table string, as ...string) *Builder[FieldType] {
	// Joins append to the existing fromClause, they don't reset it.
	fromStrBldr := b.fromClause
	fromStrBldr.WriteString(" JOIN ")
	fromStrBldr.WriteString(table)
	if len(as) == 1 {
		fromStrBldr.WriteString(" ")
		fromStrBldr.WriteString(as[0])
	}
	return b
}

// Where starts or replaces the WHERE clause with a simple column condition.
func (b *Builder[FieldType]) Where(col FieldType) *AfterWhere[FieldType] {
	wbStrBldr := b.whereClause
	wbStrBldr.Reset() // Reset the where clause
	wbStrBldr.WriteString(" WHERE ")
	b.writeColumnTo(wbStrBldr, col)
	return b.afterWhere
}

// WhereLower starts or replaces the WHERE clause with a LOWER(column) condition.
func (b *Builder[FieldType]) WhereLower(col FieldType) *AfterWhere[FieldType] {
	wbStrBldr := b.whereClause
	wbStrBldr.Reset() // Reset the where clause
	wbStrBldr.WriteString(" WHERE LOWER(")
	b.writeColumnTo(wbStrBldr, col)
	wbStrBldr.WriteString(")")
	return b.afterWhere
}

// WhereUpper starts or replaces the WHERE clause with an UPPER(column) condition.
func (b *Builder[FieldType]) WhereUpper(col FieldType) *AfterWhere[FieldType] {
	wbStrBldr := b.whereClause
	wbStrBldr.Reset() // Reset the where clause
	wbStrBldr.WriteString(" WHERE UPPER(")
	b.writeColumnTo(wbStrBldr, col)
	wbStrBldr.WriteString(")")
	return b.afterWhere
}

// And adds an "AND column" condition to the WHERE clause.
func (b *Builder[FieldType]) And(col FieldType) *AfterWhere[FieldType] {
	b.whereClause.WriteString(" AND ")
	b.writeColumnTo(b.whereClause, col)
	return b.afterWhere
}

// AndLower adds an "AND LOWER(column)" condition to the WHERE clause.
func (b *Builder[FieldType]) AndLower(col FieldType) *AfterWhere[FieldType] {
	b.whereClause.WriteString(" AND LOWER(")
	b.writeColumnTo(b.whereClause, col)
	b.whereClause.WriteString(")")
	return b.afterWhere
}

// AndUpper adds an "AND UPPER(column)" condition to the WHERE clause.
func (b *Builder[FieldType]) AndUpper(col FieldType) *AfterWhere[FieldType] {
	b.whereClause.WriteString(" AND UPPER(")
	b.writeColumnTo(b.whereClause, col)
	b.whereClause.WriteString(")")
	return b.afterWhere
}

// OrLower adds an "OR LOWER(column)" condition to the WHERE clause.
func (b *Builder[FieldType]) OrLower(col FieldType) *AfterWhere[FieldType] {
	b.whereClause.WriteString(" OR LOWER(")
	b.writeColumnTo(b.whereClause, col)
	b.whereClause.WriteString(")")
	return b.afterWhere
}

// OrUpper adds an "OR UPPER(column)" condition to the WHERE clause.
func (b *Builder[FieldType]) OrUpper(col FieldType) *AfterWhere[FieldType] {
	b.whereClause.WriteString(" OR UPPER(")
	b.writeColumnTo(b.whereClause, col)
	b.whereClause.WriteString(")")
	return b.afterWhere
}

// Eq adds an "= @param" condition to the WHERE clause.
func (b *AfterWhere[FieldType]) Eq(value any) *Builder[FieldType] {
	b.whereClause.WriteString(" = ")
	b.whereClause.WriteString(b.addParam(value))
	return b.Builder
}

// Is adds an "IS @param" condition to the WHERE clause.
func (b *AfterWhere[FieldType]) Is(value any) *Builder[FieldType] {
	b.whereClause.WriteString(" IS ")
	b.whereClause.WriteString(b.addParam(value))
	return b.Builder
}

// NotNull adds an "IS NOT NULL" condition to the WHERE clause.
func (b *AfterWhere[FieldType]) NotNull() *Builder[FieldType] {
	b.whereClause.WriteString(" IS NOT NULL")
	return b.Builder
}

// IsNull adds an "IS NULL" condition to the WHERE clause.
func (b *AfterWhere[FieldType]) IsNull() *Builder[FieldType] {
	b.whereClause.WriteString(" IS NULL")
	return b.Builder
}

// Or adds an "OR column" condition to the WHERE clause.
func (b *Builder[FieldType]) Or(col FieldType) *AfterWhere[FieldType] {
	b.whereClause.WriteString(" OR ")
	b.writeColumnTo(b.whereClause, col)
	return b.afterWhere
}

// NotEqual adds a "!= @param" condition to the WHERE clause.
func (b *AfterWhere[FieldType]) NotEqual(value any) *Builder[FieldType] {
	b.whereClause.WriteString(" != ")
	b.whereClause.WriteString(b.addParam(value))
	return b.Builder
}

// Unnest adds an "IN UNNEST(@param)" condition to the WHERE clause.
func (b *AfterWhere[FieldType]) Unnest(values any) *Builder[FieldType] {
	b.whereClause.WriteString(" IN UNNEST(")
	b.whereClause.WriteString(b.addParam(values))
	b.whereClause.WriteString(")")
	return b.Builder
}

// In adds an "IN (@param1, @param2, ...)" condition to the WHERE clause.
func (b *AfterWhere[FieldType]) In(values ...any) *Builder[FieldType] {
	if len(values) == 0 {
		return b.Builder // Or handle error: IN clause requires at least one value
	}
	wc := b.whereClause
	wc.WriteString(" IN (")
	for i, v := range values {
		if i > 0 {
			wc.WriteString(", ")
		}
		wc.WriteString(b.addParam(v))
	}
	wc.WriteString(")")
	return b.Builder
}

// LessThan adds a "< @param" condition to the WHERE clause.
func (b *AfterWhere[FieldType]) LessThan(value any) *Builder[FieldType] {
	b.whereClause.WriteString(" < ")
	b.whereClause.WriteString(b.addParam(value))
	return b.Builder
}

// GrThan adds a "> @param" condition to the WHERE clause.
func (b *AfterWhere[FieldType]) GrThan(value any) *Builder[FieldType] {
	b.whereClause.WriteString(" > ")
	b.whereClause.WriteString(b.addParam(value))
	return b.Builder
}

// LessThanOrEq adds a "<= @param" condition to the WHERE clause.
func (b *AfterWhere[FieldType]) LessThanOrEq(value any) *Builder[FieldType] {
	b.whereClause.WriteString(" <= ")
	b.whereClause.WriteString(b.addParam(value))
	return b.Builder
}

// GrThanOrEq adds a ">= @param" condition to the WHERE clause.
func (b *AfterWhere[FieldType]) GrThanOrEq(value any) *Builder[FieldType] {
	b.whereClause.WriteString(" >= ")
	b.whereClause.WriteString(b.addParam(value))
	return b.Builder
}

// Like adds a "LIKE @param" condition to the WHERE clause.
func (b *AfterWhere[FieldType]) Like(pattern any) *Builder[FieldType] {
	b.whereClause.WriteString(" LIKE ")
	b.whereClause.WriteString(b.addParam(pattern))
	return b.Builder
}

// LikeLower adds a "LIKE LOWER(@param)" condition to the WHERE clause.
// This implies the column being compared should also be LOWERed, e.g., WhereLower(col).LikeLower(pattern)
func (b *AfterWhere[FieldType]) LikeLower(pattern any) *Builder[FieldType] {
	wc := b.whereClause
	wc.WriteString(" LIKE LOWER(")
	wc.WriteString(b.addParam(pattern))
	wc.WriteString(")")
	return b.Builder
}

// Between adds a "BETWEEN @param1 AND @param2" condition to the WHERE clause.
func (b *AfterWhere[FieldType]) Between(val1 any, val2 any) *Builder[FieldType] {
	wc := b.whereClause
	wc.WriteString(" BETWEEN ")
	wc.WriteString(b.addParam(val1))
	wc.WriteString(" AND ")
	wc.WriteString(b.addParam(val2))
	return b.Builder
}

// GroupBy starts or replaces the GROUP BY clause.
func (b *Builder[FieldType]) GroupBy(col FieldType, cols ...FieldType) *Builder[FieldType] {
	gbbStrBldr := b.groupByClause
	gbbStrBldr.Reset() // Reset the group by clause
	gbbStrBldr.WriteString(" GROUP BY ")
	b.writeColumnTo(gbbStrBldr, col)
	for _, c := range cols {
		gbbStrBldr.WriteString(", ")
		b.writeColumnTo(gbbStrBldr, c)
	}
	return b
}

// ThenBy (for GroupBy) adds another column to the GROUP BY clause.
// Call after GroupBy.
func (b *Builder[FieldType]) ThenBy(col FieldType) *Builder[FieldType] {
	// This method name is now ambiguous, better to have specific ThenByGroupBy or ThenByOrderBy
	// For now, assuming it's for GroupBy if called without OrderBy first, or make it specific.
	// Let's assume this is ThenBy for GroupBy for now, if groupByClause is active.
	// A more robust API would differentiate.
	// This implementation will just append to groupByClause if it's not empty.
	if b.groupByClause.Len() > 0 {
		b.groupByClause.WriteString(", ")
		b.writeColumnTo(b.groupByClause, col)
	}
	// If you want this to be ThenBy for OrderBy, it should append to orderByClause.
	// To avoid ambiguity, it's better to have distinct ThenByGroupBy and ThenByOrderBy methods.
	// For this refactor, I'll keep a single ThenBy and it will append to GroupBy if GroupBy was started.
	// If OrderBy was started and GroupBy was not, it could append to OrderBy. This is messy.

	// Corrected approach: ThenBy should be context-aware or we need separate methods.
	// Given the simplification, let's make ThenBy apply to OrderBy as it's more common.
	// If you need ThenBy for GroupBy, you can call GroupBy with multiple columns.
	// So, this ThenBy will target orderByClause.
	if b.orderByClause.Len() > 0 { // Check if ORDER BY clause has been started
		b.orderByClause.WriteString(", ")
		b.writeColumnTo(b.orderByClause, col)
	}
	return b
}

// Having adds a HAVING clause to the GROUP BY clause.
// This is typically used after a GROUP BY to filter groups based on aggregate functions.
func (b *Builder[FieldType]) Having(condition string) *Builder[FieldType] {
	gbc := b.groupByClause
	if gbc.Len() > 0 {
		if !strings.Contains(gbc.String(), " HAVING ") {
			gbc.WriteString(" HAVING ")
		} else {
			gbc.WriteString(" AND ")
		}
		gbc.WriteString(condition)
	}
	return b
}

func (b *Builder[FieldType]) OrderBy(col FieldType, cols ...FieldType) *Builder[FieldType] {
	obbStrBldr := b.orderByClause
	obbStrBldr.Reset()
	obbStrBldr.WriteString(" ORDER BY ")
	b.writeColumnTo(obbStrBldr, col)
	for _, c := range cols {
		obbStrBldr.WriteString(", ")
		b.writeColumnTo(obbStrBldr, c)
	}
	return b
}

func (b *Builder[FieldType]) Asc() *Builder[FieldType] {
	if b.orderByClause.Len() > 0 {
		b.orderByClause.WriteString(" ASC")
	}
	return b
}

func (b *Builder[FieldType]) Desc() *Builder[FieldType] {
	if b.orderByClause.Len() > 0 {
		b.orderByClause.WriteString(" DESC")
	}
	return b
}

func (b *Builder[FieldType]) Limit(val int) *Builder[FieldType] {
	lbStrBldr := b.limitClause
	lbStrBldr.Reset()
	lbStrBldr.WriteString(" LIMIT ")
	lbStrBldr.WriteString(strconv.Itoa(val))
	return b
}

func (b *Builder[FieldType]) Offset(val int) *Builder[FieldType] {
	obStrBldr := b.offsetClause
	obStrBldr.Reset()
	obStrBldr.WriteString(" OFFSET ")
	obStrBldr.WriteString(strconv.Itoa(val))
	return b
}

func (b *Builder[FieldType]) String() string {
	return b.selectClause.String() +
		b.fromClause.String() +
		b.whereClause.String() +
		b.groupByClause.String() +
		b.orderByClause.String() +
		b.limitClause.String() +
		b.offsetClause.String()
}

func (b *Builder[FieldType]) Params() map[string]any {
	return b.params
}

func (b *Builder[FieldType]) Fields() []FieldType {
	if len(b.fields) == 0 {
		return nil // Return nil if no fields were selected
	}
	return b.fields
}

func (b *Builder[FieldType]) Reset() *Builder[FieldType] {
	if b == nil {
		return b
	}
	b.selectClause.Reset()
	b.fromClause.Reset()
	b.whereClause.Reset()
	b.orderByClause.Reset()
	b.groupByClause.Reset()
	b.limitClause.Reset()
	b.offsetClause.Reset()

	b.params = make(map[string]any)
	if b.paramWriter != nil {
		b.paramWriter.Reset()
	}
	return b
}
`

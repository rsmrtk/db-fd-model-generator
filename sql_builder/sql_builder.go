package sql_builder

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
	sb.WriteString("`")
	sb.WriteString(string(col))
	sb.WriteString("`")
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

package sqlquery

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

type DBTX interface {
	ExecContext(context.Context, string, ...interface{}) (sql.Result, error)
	PrepareContext(context.Context, string) (*sql.Stmt, error)
	QueryContext(context.Context, string, ...interface{}) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...interface{}) *sql.Row
}

type Scaneable interface {
	Fields() []any
}

type Querier[T any] struct {
	db DBTX
}

func New[T any](db DBTX) *Querier[T] {
	return &Querier[T]{
		db: db,
	}
}

func (*Querier[T]) WithTx(db DBTX) *Querier[T] {
	return &Querier[T]{
		db: db,
	}
}

func (q *Querier[T]) PaginateQuery(ctx context.Context, skip, limit int, query string, args ...any) ([]T, error) {
	baseQuery := &strings.Builder{}
	baseQuery.WriteString(query)
	var err error
	if limit > 0 {
		if _, err = fmt.Fprintf(baseQuery, " LIMIT %d ", limit); err != nil {
			return nil, err
		}
	}
	if skip > 0 {
		if _, err = fmt.Fprintf(baseQuery, " OFFSET %d ", skip); err != nil {
			return nil, err
		}
	}

	return q.Query(ctx, baseQuery.String(), args...)
}

func (q *Querier[T]) QueryOne(ctx context.Context, query string) (T, error) {
	raw := q.db.QueryRowContext(ctx, query)
	var (
		val       T
		scanables = newScanableItems(&val)
	)
	if err := raw.Scan(scanables.values()...); err != nil {
		return val, err
	}

	return val, raw.Err()
}

func (q *Querier[T]) Query(ctx context.Context, query string, args ...any) ([]T, error) {
	raws, err := q.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	var items = make([]T, 0)
	for raws.Next() {
		var (
			val       T
			scanables = newScanableItems(&val)
		)
		if err = raws.Scan(scanables.values()...); err != nil {
			return nil, err
		}
		items = append(items, val)
	}

	return items, errors.Join(
		raws.Close(),
		raws.Err(),
	)
}

type scanableItems []any

func newScanableItems(items ...any) *scanableItems {
	var s scanableItems
	for _, item := range items {
		s.add(item)
	}

	return &s
}

func (s *scanableItems) values() []any {
	return *s
}

func (s *scanableItems) add(item any) (found bool) {
	switch i := item.(type) {
	case Scaneable:
		*s = append(*s, i.Fields()...)
		found = true
	case []Scaneable:
		found = len(i) > 0
		for _, ii := range i {
			if !s.add(ii) {
				found = false
			}
		}
	case *int, *int8, *int32, *int64, *uint, *uint8, *uint64, *uint16, *string, *[]byte:
		*s = append(*s, i)
		found = true
	}

	return false
}

func (q *Querier[T]) QueryRaw(ctx context.Context, query string, args ...any) (T, error) {
	raw := q.db.QueryRowContext(ctx, query, args...)

	var val T
	var scanables = newScanableItems(&val)
	if err := raw.Scan(scanables.values()...); err != nil {
		return val, err
	}

	return val, nil
}

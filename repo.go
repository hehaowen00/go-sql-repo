package gosqlrepo

import (
	"database/sql"
	"fmt"
	"slices"
	"strings"
)

type DBInterface interface {
	Query(stmt string, args ...any) (*sql.Rows, error)
	QueryRow(stmt string, args ...any) *sql.Row
	Exec(stmt string, args ...any) (sql.Result, error)
}

type Accessor[T any] func(*T) any

type SQLMap[T any] map[string]Accessor[T]

type IMapper[T any] interface {
	Mapper() SQLMap[T]
}

type SQLRepo[T any] struct {
	db        *sql.DB
	table     string
	pks       []string
	keys      []string
	accessors []Accessor[T]
}

func NewSQLRepo[T IMapper[T]](
	db *sql.DB,
	table string,
	pks []string,
) *SQLRepo[T] {
	var empty T
	mapper := empty.Mapper()
	keys := []string{}
	accessors := []Accessor[T]{}

	for k, v := range mapper {
		keys = append(keys, k)
		accessors = append(accessors, v)
	}

	return &SQLRepo[T]{
		db:        db,
		table:     table,
		keys:      keys,
		pks:       pks,
		accessors: accessors,
	}
}

func (r *SQLRepo[T]) DB() *sql.DB {
	return r.db
}

func (r *SQLRepo[T]) Count(db DBInterface, stmt string, args ...any) (int, error) {
	count := 0

	err := queryRow(db, fmt.Sprintf("SELECT COUNT(*) FROM %s %s", r.table, stmt), args...).Scan(&count)
	if err != nil {
		return 0, err
	}

	return count, nil
}

func (r *SQLRepo[T]) Select(
	db DBInterface,
	suffix string,
	args ...any,
) ([]*T, error) {
	rows, err := query(
		db,
		fmt.Sprintf("SELECT %s FROM %s %s", strings.Join(r.keys, ", "), r.table, suffix),
		args...,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	res := []*T{}

	for rows.Next() {
		var item T

		values := []any{}
		for _, f := range r.accessors {
			values = append(values, f(&item))
		}

		err = rows.Scan(values...)
		if err != nil {
			panic(err)
		}

		res = append(res, &item)
	}

	return res, nil
}

func (r *SQLRepo[T]) SelectOne(
	db DBInterface,
	suffix string,
	args ...any,
) (*T, error) {
	var res T

	dest := []any{}
	for _, f := range r.accessors {
		dest = append(dest, f(&res))
	}

	err := queryRow(
		db,
		fmt.Sprintf("SELECT %s FROM %s %s LIMIT 1", strings.Join(r.keys, ", "), r.table, suffix), args...).Scan(dest...)
	if err != nil {
		return nil, err
	}

	return &res, nil
}
func (r *SQLRepo[T]) Insert(
	db DBInterface,
	item *T,
) error {
	values := []any{}
	placeholders := []string{}

	for i, f := range r.accessors {
		values = append(values, f(item))
		placeholders = append(placeholders, fmt.Sprintf("$c%d", i+1))
	}

	err := exec(
		db,
		fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", r.table, strings.Join(r.keys, ", "), strings.Join(placeholders, ", ")),
		values...,
	)

	return err
}

func (r *SQLRepo[T]) Update(
	db DBInterface,
	stmt string,
	args ...any,
) error {
	err := exec(db, fmt.Sprintf("UPDATE %s %s", r.table, stmt), args...)
	return err
}

func (r *SQLRepo[T]) Upsert(
	db DBInterface,
	item *T,
	conflict []string,
) error {
	values := []any{}
	placeholders := []string{}

	for i, f := range r.accessors {
		values = append(values, f(item))
		placeholders = append(placeholders, fmt.Sprintf("$c%d", i+1))
	}

	setters := []string{}
	for i, k := range r.keys {
		if slices.Contains(r.pks, k) {
			continue
		}
		if slices.Contains(conflict, k) {
			continue
		}

		setters = append(setters, fmt.Sprintf("%s = $c%d", k, i+1))
	}

	err := exec(
		db,
		fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) DO UPDATE SET %s",
			r.table,
			strings.Join(r.keys, ", "),
			strings.Join(placeholders, ", "),
			strings.Join(r.pks, ", "),
			strings.Join(setters, ", "),
		),
		values...,
	)

	return err
}

func (r *SQLRepo[T]) Delete(
	db DBInterface,
	suffix string,
	args ...any,
) error {
	err := exec(db, fmt.Sprintf("DELETE FROM %s %s", r.table, suffix), args...)
	return err
}

func wrapParams(args ...any) []any {
	values := []any{}
	for i, v := range args {
		values = append(values, sql.Named(fmt.Sprintf("c%d", i+1), v))
	}
	return values
}

func query(db DBInterface, stmt string, args ...any) (*sql.Rows, error) {
	if len(args) > 0 {
		params := wrapParams(args...)
		return db.Query(stmt, params...)
	}

	return db.Query(stmt)
}

func queryRow(db DBInterface, stmt string, args ...any) *sql.Row {
	if len(args) > 0 {
		params := wrapParams(args)
		return db.QueryRow(stmt, params...)
	}

	return db.QueryRow(stmt)
}
func exec(db DBInterface, stmt string, args ...any) error {
	_, err := db.Exec(stmt, args...)
	return err
}

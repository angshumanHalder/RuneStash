# RuneStash

A persistent, transactional relational database built from scratch in Go. Implements a B+tree storage engine, a KV store, a table layer with secondary indexes, concurrent transactions, and a SQL-like query language.

Built following [Build Your Own Database From Scratch](https://build-your-own.org/database/).

## Architecture

```
SQL Parser  (parser.go)
    ↓
Query Executor  (query.go, eval.go)
    ↓
Table Layer  (db.go, table.go)       ← rows, schemas, secondary indexes
    ↓
KV Store  (kv.go)                    ← transactions, concurrency control
    ↓
B+Tree  (tree.go, node.go)           ← copy-on-write, range queries
    ↓
Pager  (pager.go, meta.go)           ← mmap, free list, fsync durability
```

## Features

- **B+tree** — copy-on-write, range queries, bidirectional iteration
- **KV store** — ACID transactions, snapshot isolation, optimistic concurrency control
- **Free list** — page recycling via on-disk linked list
- **Tables** — typed columns (`int64`, `string`), primary keys, secondary indexes
- **Order-preserving encoding** — enables range queries on multi-column keys
- **SQL-like query language** — `SELECT`, `INSERT`, `UPDATE`, `DELETE`, `CREATE TABLE`

## Query Language

### Statements

```sql
create table users (
    id     int,
    name   string,
    age    int,
    index (age),
    primary key (id)
);

insert into users (id, name, age) values (1, 'alice', 30);

select id, name from users index by age > 18 filter name = 'alice' limit 10;

update users set age = 31 filter id = 1;

delete from users filter age < 18;
```

### Clauses

| Clause | Description |
|--------|-------------|
| `INDEX BY col = val` | point lookup on index |
| `INDEX BY col > val` | open-ended range scan |
| `INDEX BY col > a and col < b` | bounded range scan |
| `FILTER expr` | post-scan row filter (arbitrary expression) |
| `LIMIT n` | return at most n rows |
| `LIMIT offset, n` | skip offset rows, return n |

### Expressions

```
a OR b
a AND b
NOT a
a = b,  a != b,  a < b,  a > b,  a <= b,  a >= b
a + b,  a - b,  a * b,  a / b
-a
```

## REPL

```
$ go build -o runestash .
$ ./runestash mydb.db

RuneStash — mydb.db
> create table users (id int, name string, primary key (id));
OK
> insert into users (id, name) values (1, 'alice'), (2, 'bob');
OK
> select id, name from users;
id      name
----------------------------------------
1       alice
2       bob
(2 rows)
> \tables
users
(1 tables)
> \q
```

### REPL Commands

| Command | Description |
|---------|-------------|
| `\tables` | list all user tables |
| `\q` / `exit` / `quit` | exit |
| `;` | terminates a SQL statement (multi-line supported) |

## Running Tests

```
go test ./...
```

## Data Types

| SQL type | Go type |
|----------|---------|
| `int` / `int64` / `integer` | `int64` |
| `string` / `text` / `bytes` | `[]byte` |

## Durability

Writes are durable via `fsync`. The storage engine uses copy-on-write so partial writes never corrupt existing data. The meta page is updated atomically after each commit.

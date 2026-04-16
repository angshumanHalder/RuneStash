package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: runestash <db-path>")
		os.Exit(1)
	}

	db := DB{Path: os.Args[1]}
	if err := db.Open(); err != nil {
		fmt.Fprintf(os.Stderr, "open: %v\n", err)
		os.Exit(1)
	}
	defer db.kv.Close()

	fmt.Printf("RuneStash — %s\n", os.Args[1])

	scanner := bufio.NewScanner(os.Stdin)
	var buf strings.Builder

	for {
		if buf.Len() == 0 {
			fmt.Print("> ")
		} else {
			fmt.Print("  ")
		}

		if !scanner.Scan() {
			break
		}
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		if line == "\\q" || line == "exit" || line == "quit" {
			break
		}
		if line == "\\tables" {
			listTables(&db)
			continue
		}

		buf.WriteString(line)
		buf.WriteByte(' ')

		// execute once we see a semicolon
		if !strings.Contains(line, ";") {
			continue
		}

		input := strings.TrimSpace(buf.String())
		buf.Reset()

		iter, err := ExecSQL(&db, input)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			continue
		}
		if iter == nil {
			fmt.Println("OK")
			continue
		}
		printResults(iter)
	}
}

func listTables(db *DB) {
	sc := Scanner{Cmp1: CmpGE, Cmp2: CmpLE}
	if err := dbScan(db, TDefTable, &sc); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		return
	}
	count := 0
	for ; sc.Valid(); sc.Next() {
		var rec Record
		sc.Deref(&rec)
		v := rec.Get("name")
		if v != nil {
			fmt.Println(string(v.Str))
			count++
		}
	}
	fmt.Printf("(%d tables)\n", count)
}

func printResults(iter RecordIter) {
	count := 0
	for ; iter.Valid(); iter.Next() {
		var rec Record
		if err := iter.Deref(&rec); err != nil {
			fmt.Fprintf(os.Stderr, "deref: %v\n", err)
			return
		}
		if count == 0 {
			// header
			fmt.Println(strings.Join(rec.Cols, "\t"))
			fmt.Println(strings.Repeat("-", 40))
		}
		row := make([]string, len(rec.Vals))
		for i, v := range rec.Vals {
			switch v.Type {
			case TypeInt64:
				row[i] = fmt.Sprintf("%d", v.I64)
			case TypeBytes:
				row[i] = string(v.Str)
			default:
				row[i] = "?"
			}
		}
		fmt.Println(strings.Join(row, "\t"))
		count++
	}
	fmt.Printf("(%d rows)\n", count)
}

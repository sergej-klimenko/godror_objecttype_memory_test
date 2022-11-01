package main

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"runtime"
	"strconv"

	"github.com/godror/godror"
)

func main() {
	//godror.SetLogger(godror.NewLogfmtLogger(os.Stdout))

	testConStr := os.Getenv("GODROR_TEST_DSN")
	runs, _ := strconv.Atoi(os.Getenv("RUNS"))
	step, _ := strconv.Atoi(os.Getenv("STEP"))

	ctx := context.Background()

	P, err := godror.ParseDSN(testConStr)
	if err != nil {
		log.Fatalf("%q: %+v", testConStr, err)
	}

	fmt.Printf(
		"connect using: standaloneConnection=%v, maxSessions=%v\n",
		P.StandaloneConnection, P.MaxSessions,
	)

	db := sql.OpenDB(godror.NewConnector(P))
	defer db.Close()

	err = createTypes(ctx, db)
	if err != nil {
		log.Fatal(err)
	}

	cx, err := db.Conn(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer cx.Close()

	var m runtime.MemStats
	pid := os.Getpid()

	loopCnt := 0
	printStats := func() {
		runtime.GC()
		runtime.ReadMemStats(&m)
		log.Printf("Alloc: %.3f MiB, Heap: %.3f MiB, Sys: %.3f MiB, NumGC: %d\n",
			float64(m.Alloc)/1024/1024, float64(m.HeapInuse)/1024/1024, float64(m.Sys)/1024/1024, m.NumGC)

		rss, err := readMem(int32(pid))
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("%d; process memory (rss): %.3f MiB\n", loopCnt, float64(rss)/1024/1024)

	}

	for ; loopCnt < runs; loopCnt++ {
		err = callObjectType(ctx, cx)
		if err != nil {
			log.Fatal(err)
		}

		if loopCnt%step == 0 {
			printStats()
		}
	}
	printStats()
}

func callObjectType(ctx context.Context, cx *sql.Conn) error {
	objType, err := godror.GetObjectType(ctx, cx, "TEST_TYPE")
	if err != nil {
		return err
	}

	obj, err := objType.NewObject()
	if err != nil {
		return err
	}
	defer obj.Close()

	rec := MyObject{Object: obj, ID: 1}
	params := []interface{}{
		sql.Named("rec", sql.Out{Dest: &rec, In: true}),
	}
	_, err = cx.ExecContext(ctx, `begin test_pkg_sample.test_record_in(:rec); end;`, params...)

	return err
}

func createTypes(ctx context.Context, db *sql.DB) error {
	qry := []string{
		`create or replace type test_type force as object (
   	  id    number(10)
    );`,
		`CREATE OR REPLACE PACKAGE test_pkg_sample AS
	PROCEDURE test_record_in (
		rec IN OUT test_type
	);
	END test_pkg_sample;`,
		`CREATE OR REPLACE PACKAGE BODY test_pkg_sample AS
	PROCEDURE test_record_in (
		rec IN OUT test_type
	) IS
	BEGIN
		rec.id := rec.id + 1;
	END test_record_in;
	END test_pkg_sample;`,
	}
	for _, ddl := range qry {
		_, err := db.ExecContext(ctx, ddl)
		if err != nil {
			return err
		}
	}
	return nil
}

func readMem(pid int32) (uint64, error) {
	b, err := os.ReadFile("/proc/" + strconv.FormatInt(int64(pid), 10) + "/status")
	if err != nil {
		return 0, err
	}
	if i := bytes.Index(b, []byte("\nRssAnon:")); i >= 0 {
		b = b[i+1+3+4+1+1:]
		if i = bytes.IndexByte(b, '\n'); i >= 0 {
			b = b[:i]
			var n uint64
			var u string
			_, err := fmt.Sscanf(string(b), "%d %s", &n, &u)
			if err != nil {
				return 0, fmt.Errorf("%s: %w", string(b), err)
			}
			switch u {
			case "kB":
				n <<= 10
			case "MB":
				n <<= 20
			case "GB":
				n <<= 30
			}
			return n, nil
		}
	}
	return 0, nil
}

type MyObject struct {
	*godror.Object
	ID int64
}

func (r *MyObject) Scan(src interface{}) error {
	obj, ok := src.(*godror.Object)
	if !ok {
		return fmt.Errorf("Cannot scan from type %T", src)
	}
	id, err := obj.Get("ID")
	if err != nil {
		return err
	}
	r.ID = id.(int64)

	return nil
}

// WriteObject update godror.Object with struct attributes values.
// Implement this method if you need the record as an input parameter.
func (r MyObject) WriteObject() error {
	// all attributes must be initialized or you get an "ORA-21525: attribute number or (collection element at index) %s violated its constraints"
	err := r.ResetAttributes()
	if err != nil {
		return err
	}

	var data godror.Data
	err = r.GetAttribute(&data, "ID")
	if err != nil {
		return err
	}
	data.SetInt64(r.ID)
	r.SetAttribute("ID", &data)

	return nil
}

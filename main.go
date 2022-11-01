package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"runtime"
	"strconv"
	"sync"
	"time"

	"github.com/godror/godror"
	"github.com/shirou/gopsutil/process"
)

var (
	parallel             = 1
	standaloneConnection = false
)

func main() {
	testConStr := os.Getenv("GODROR_TEST_DSN")
	runs, _ := strconv.Atoi(os.Getenv("RUNS"))
	step, _ := strconv.Atoi(os.Getenv("STEP"))
	ctx := context.Background()

	P, err := godror.ParseDSN(testConStr)
	if err != nil {
		log.Fatalf("%q: %+v", testConStr, err)
	}

	standaloneConnection = P.StandaloneConnection

	fmt.Printf(
		"connect using: standaloneConnection=%v, maxSessions=%v\n",
		standaloneConnection, P.MaxSessions,
	)

	db := sql.OpenDB(godror.NewConnector(P))
	defer db.Close()

	err = createTypes(ctx, db)
	if err != nil {
		log.Fatal(err)
	}

	var m runtime.MemStats
	pid := os.Getpid()
	for loopCnt := 0; loopCnt < runs; loopCnt++ {
		var wg sync.WaitGroup
		wg.Add(parallel)

		for i := 0; i < parallel; i++ {
			go func(wg *sync.WaitGroup) {
				defer wg.Done()
				err := callObjectType(ctx, db)
				if err != nil {
					log.Fatal(err)
				}
			}(&wg)
		}
		wg.Wait()

		if loopCnt%step == 0 {
			runtime.ReadMemStats(&m)
			log.Printf("Alloc: %.3f MiB, Heap: %.3f MiB, Sys: %.3f MiB, NumGC: %d\n",
				float64(m.Alloc)/1024/1024, float64(m.HeapInuse)/1024/1024, float64(m.Sys)/1024/1024, m.NumGC)

			rss, err := readMem(int32(pid))
			if err != nil {
				log.Fatal(err)
			}
			log.Printf("%d; process memory (rss): %.3f MiB\n", loopCnt, float64(rss)/1024/1024)

			time.Sleep(100 * time.Millisecond)
		}
	}
}

func callObjectType(ctx context.Context, db *sql.DB) error {
	cx, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer cx.Close()

	tx, err := cx.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Commit()

	objType, err := godror.GetObjectType(ctx, tx, "TEST_TYPE")
	if err != nil {
		return err
	}

	if !standaloneConnection {
		defer objType.Close()
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
	_, err = tx.ExecContext(ctx, `begin test_pkg_sample.test_record_in(:rec); end;`, params...)

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
	p, err := process.NewProcess(pid)
	if err != nil {
		return 0, err
	}

	m, err := p.MemoryInfo()
	if err != nil {
		return 0, err
	}

	//fmt.Println(m)
	return m.RSS, nil
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

package main

import (
	"context"
	"database/sql"
	"log"
	"os"
	"runtime"
	"strconv"
	"sync"
	"time"

	"github.com/godror/godror"
	"github.com/shirou/gopsutil/process"
)

var parallel = 1

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
	for loopCnt := 0; loopCnt < runs; loopCnt++ {
		var wg sync.WaitGroup
		wg.Add(parallel)

		for i := 0; i < parallel; i++ {
			go func(wg *sync.WaitGroup) {
				defer wg.Done()
				err := callObjectType(ctx, cx)
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

func callObjectType(ctx context.Context, cx *sql.Conn) error {
	objType, err := godror.GetObjectType(ctx, cx, "TEST_TYPE")
	if err != nil {
		return err
	}
	defer objType.Close()

	return err
}

func createTypes(ctx context.Context, db *sql.DB) error {
	qry := []string{
		`create or replace type test_type force as object (
   	  id    number(10)
    );`,
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

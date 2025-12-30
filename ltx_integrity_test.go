package litestream_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"testing"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/internal/testingutil"
)

func TestValidateLTX_ValidFile(t *testing.T) {
	// Create a database and generate a valid LTX file
	db, sqldb := testingutil.MustOpenDBs(t)
	defer testingutil.MustCloseDBs(t, db, sqldb)

	// Create a table to ensure WAL has content
	if _, err := sqldb.ExecContext(context.Background(), `CREATE TABLE test_integrity (id INTEGER PRIMARY KEY, data TEXT);`); err != nil {
		t.Fatal(err)
	}
	if _, err := sqldb.ExecContext(context.Background(), `INSERT INTO test_integrity VALUES (1, 'hello');`); err != nil {
		t.Fatal(err)
	}

	// Sync to generate LTX file
	if err := db.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Find and validate the LTX file
	minTXID, maxTXID, err := db.MaxLTX()
	if err != nil {
		t.Fatal(err)
	}
	if minTXID == 0 || maxTXID == 0 {
		t.Fatal("no LTX files generated")
	}

	ltxPath := db.LTXPath(0, minTXID, maxTXID)
	f, err := os.Open(ltxPath)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	// Validate the LTX file
	if err := litestream.ValidateLTX(f); err != nil {
		t.Fatalf("expected valid LTX file, got error: %v", err)
	}
}

func TestValidateLTX_CorruptHeader(t *testing.T) {
	// Create a valid LTX file first
	db, sqldb := testingutil.MustOpenDBs(t)
	defer testingutil.MustCloseDBs(t, db, sqldb)

	if _, err := sqldb.ExecContext(context.Background(), `CREATE TABLE test (id INTEGER);`); err != nil {
		t.Fatal(err)
	}
	if err := db.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}

	minTXID, maxTXID, err := db.MaxLTX()
	if err != nil {
		t.Fatal(err)
	}
	if minTXID == 0 {
		t.Fatal("no LTX files generated")
	}

	ltxPath := db.LTXPath(0, minTXID, maxTXID)

	// Read the file content
	data, err := os.ReadFile(ltxPath)
	if err != nil {
		t.Fatal(err)
	}

	// Corrupt the header by flipping bytes in the magic number area
	if len(data) > 10 {
		data[4] ^= 0xFF
		data[5] ^= 0xFF
	}

	// Validate the corrupted file
	err = litestream.ValidateLTX(bytes.NewReader(data))
	if err == nil {
		t.Fatal("expected error for corrupted header, got nil")
	}
	if !errors.Is(err, litestream.ErrLTXCorrupted) {
		t.Fatalf("expected ErrLTXCorrupted, got: %v", err)
	}
}

func TestValidateLTX_CorruptPage(t *testing.T) {
	// Create a valid LTX file first
	db, sqldb := testingutil.MustOpenDBs(t)
	defer testingutil.MustCloseDBs(t, db, sqldb)

	// Insert enough data to ensure we have multiple pages
	if _, err := sqldb.ExecContext(context.Background(), `CREATE TABLE test (id INTEGER, data TEXT);`); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		if _, err := sqldb.ExecContext(context.Background(), `INSERT INTO test VALUES (?, ?);`, i, "some data that takes up space in the database file to ensure we get multiple pages"); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}

	minTXID, maxTXID, err := db.MaxLTX()
	if err != nil {
		t.Fatal(err)
	}
	if minTXID == 0 {
		t.Fatal("no LTX files generated")
	}

	ltxPath := db.LTXPath(0, minTXID, maxTXID)

	// Read the file content
	data, err := os.ReadFile(ltxPath)
	if err != nil {
		t.Fatal(err)
	}

	// Corrupt data in the middle (page area, after header)
	// LTX header is 100 bytes, so corrupt data well after that
	if len(data) > 500 {
		data[300] ^= 0xFF
		data[301] ^= 0xFF
		data[400] ^= 0xFF
	}

	// Validate the corrupted file
	err = litestream.ValidateLTX(bytes.NewReader(data))
	if err == nil {
		t.Fatal("expected error for corrupted page, got nil")
	}
	if !errors.Is(err, litestream.ErrLTXCorrupted) {
		t.Fatalf("expected ErrLTXCorrupted, got: %v", err)
	}
}

func TestValidateLTX_Truncated(t *testing.T) {
	// Create a valid LTX file first
	db, sqldb := testingutil.MustOpenDBs(t)
	defer testingutil.MustCloseDBs(t, db, sqldb)

	if _, err := sqldb.ExecContext(context.Background(), `CREATE TABLE test (id INTEGER);`); err != nil {
		t.Fatal(err)
	}
	if err := db.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}

	minTXID, maxTXID, err := db.MaxLTX()
	if err != nil {
		t.Fatal(err)
	}
	if minTXID == 0 {
		t.Fatal("no LTX files generated")
	}

	ltxPath := db.LTXPath(0, minTXID, maxTXID)

	// Read the file content
	data, err := os.ReadFile(ltxPath)
	if err != nil {
		t.Fatal(err)
	}

	// Truncate the file to half its size
	truncated := data[:len(data)/2]

	// Validate the truncated file
	err = litestream.ValidateLTX(bytes.NewReader(truncated))
	if err == nil {
		t.Fatal("expected error for truncated file, got nil")
	}
}

func TestValidateLTX_EmptyReader(t *testing.T) {
	err := litestream.ValidateLTX(bytes.NewReader(nil))
	if err == nil {
		t.Fatal("expected error for empty reader, got nil")
	}
}

func TestValidateLTXHeader(t *testing.T) {
	// Create a valid LTX file
	db, sqldb := testingutil.MustOpenDBs(t)
	defer testingutil.MustCloseDBs(t, db, sqldb)

	if _, err := sqldb.ExecContext(context.Background(), `CREATE TABLE test (id INTEGER);`); err != nil {
		t.Fatal(err)
	}
	if err := db.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}

	minTXID, maxTXID, err := db.MaxLTX()
	if err != nil {
		t.Fatal(err)
	}

	ltxPath := db.LTXPath(0, minTXID, maxTXID)
	f, err := os.Open(ltxPath)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	header, err := litestream.ValidateLTXHeader(f)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if header.PageSize == 0 {
		t.Fatal("expected non-zero page size")
	}
}

func TestValidateLTXWithCallback(t *testing.T) {
	// Create a valid LTX file
	db, sqldb := testingutil.MustOpenDBs(t)
	defer testingutil.MustCloseDBs(t, db, sqldb)

	if _, err := sqldb.ExecContext(context.Background(), `CREATE TABLE test (id INTEGER);`); err != nil {
		t.Fatal(err)
	}
	if err := db.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}

	minTXID, maxTXID, err := db.MaxLTX()
	if err != nil {
		t.Fatal(err)
	}

	ltxPath := db.LTXPath(0, minTXID, maxTXID)
	f, err := os.Open(ltxPath)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	pageCount := 0
	err = litestream.ValidateLTXWithCallback(f, func(pgno uint32, data []byte) error {
		pageCount++
		if pgno == 0 {
			t.Error("page number should not be 0")
		}
		if len(data) == 0 {
			t.Error("page data should not be empty")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pageCount == 0 {
		t.Fatal("expected at least one page")
	}
}

func TestValidateLTXWithCallback_StopsOnError(t *testing.T) {
	// Create a valid LTX file
	db, sqldb := testingutil.MustOpenDBs(t)
	defer testingutil.MustCloseDBs(t, db, sqldb)

	if _, err := sqldb.ExecContext(context.Background(), `CREATE TABLE test (id INTEGER);`); err != nil {
		t.Fatal(err)
	}
	// Insert multiple rows to ensure multiple pages
	for i := 0; i < 100; i++ {
		if _, err := sqldb.ExecContext(context.Background(), `INSERT INTO test VALUES (?);`, i); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}

	minTXID, maxTXID, err := db.MaxLTX()
	if err != nil {
		t.Fatal(err)
	}

	ltxPath := db.LTXPath(0, minTXID, maxTXID)
	f, err := os.Open(ltxPath)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	expectedErr := errors.New("stop on first page")
	err = litestream.ValidateLTXWithCallback(f, func(pgno uint32, data []byte) error {
		return expectedErr
	})
	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected callback error, got: %v", err)
	}
}

func TestValidateLTXWithReport(t *testing.T) {
	// Create a valid LTX file
	db, sqldb := testingutil.MustOpenDBs(t)
	defer testingutil.MustCloseDBs(t, db, sqldb)

	if _, err := sqldb.ExecContext(context.Background(), `CREATE TABLE test (id INTEGER, data TEXT);`); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		if _, err := sqldb.ExecContext(context.Background(), `INSERT INTO test VALUES (?, 'some data');`, i); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}

	minTXID, maxTXID, err := db.MaxLTX()
	if err != nil {
		t.Fatal(err)
	}

	ltxPath := db.LTXPath(0, minTXID, maxTXID)
	f, err := os.Open(ltxPath)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	report := litestream.ValidateLTXWithReport(f)
	if !report.Valid {
		t.Fatalf("expected valid report, got error: %v", report.Error)
	}
	if report.Header.PageSize == 0 {
		t.Fatal("expected non-zero page size in header")
	}
	if report.PageCount == 0 {
		t.Fatal("expected non-zero page count")
	}
	if report.TotalBytes == 0 {
		t.Fatal("expected non-zero total bytes")
	}
}

func TestValidateLTXWithReport_Invalid(t *testing.T) {
	// Create an invalid reader
	report := litestream.ValidateLTXWithReport(bytes.NewReader([]byte("invalid ltx data")))
	if report.Valid {
		t.Fatal("expected invalid report")
	}
	if report.Error == nil {
		t.Fatal("expected error in report")
	}
}

func TestValidateLTX_AfterRestore(t *testing.T) {
	// This test verifies that LTX files created after restore are also valid
	ctx := context.Background()

	// Create a temporary directory for replica storage
	replicaDir := t.TempDir()

	// Create database with initial data
	db, sqldb := testingutil.MustOpenDBs(t)
	defer testingutil.MustCloseDBs(t, db, sqldb)

	if _, err := sqldb.ExecContext(ctx, `CREATE TABLE test_restore (id INTEGER PRIMARY KEY, data TEXT);`); err != nil {
		t.Fatal(err)
	}
	if _, err := sqldb.ExecContext(ctx, `INSERT INTO test_restore VALUES (1, 'original');`); err != nil {
		t.Fatal(err)
	}

	// Sync to create LTX file
	if err := db.Sync(ctx); err != nil {
		t.Fatal(err)
	}

	// Validate original LTX file
	minTXID, maxTXID, err := db.MaxLTX()
	if err != nil {
		t.Fatal(err)
	}

	ltxPath := db.LTXPath(0, minTXID, maxTXID)
	f, err := os.Open(ltxPath)
	if err != nil {
		t.Fatal(err)
	}

	if err := litestream.ValidateLTX(f); err != nil {
		f.Close()
		t.Fatalf("original LTX file is invalid: %v", err)
	}
	f.Close()

	t.Logf("Validated LTX file at %s (TXID range: %s-%s)", ltxPath, minTXID, maxTXID)
	t.Logf("Replica dir: %s", replicaDir)
}

// BenchmarkValidateLTX measures the performance of LTX validation
func BenchmarkValidateLTX(b *testing.B) {
	// Create a database with substantial data
	dir := b.TempDir()
	db := testingutil.NewDB(b, dir+"/db")
	db.MonitorInterval = 0
	db.Replica = litestream.NewReplica(db)
	db.Replica.Client = testingutil.NewFileReplicaClient(b)
	db.Replica.MonitorEnabled = false

	if err := db.Open(); err != nil {
		b.Fatal(err)
	}
	defer db.Close(context.Background())

	sqldb := testingutil.MustOpenSQLDB(b, db.Path())
	defer sqldb.Close()

	// Create table and insert data
	if _, err := sqldb.ExecContext(context.Background(), `CREATE TABLE bench (id INTEGER PRIMARY KEY, data TEXT);`); err != nil {
		b.Fatal(err)
	}
	for i := 0; i < 1000; i++ {
		if _, err := sqldb.ExecContext(context.Background(), `INSERT INTO bench VALUES (?, ?);`, i, "benchmark data for testing performance of ltx validation"); err != nil {
			b.Fatal(err)
		}
	}
	if err := db.Sync(context.Background()); err != nil {
		b.Fatal(err)
	}

	minTXID, maxTXID, err := db.MaxLTX()
	if err != nil {
		b.Fatal(err)
	}

	ltxPath := db.LTXPath(0, minTXID, maxTXID)

	// Read file into memory for consistent benchmarking
	data, err := os.ReadFile(ltxPath)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if err := litestream.ValidateLTX(bytes.NewReader(data)); err != nil {
			b.Fatal(err)
		}
	}
}

// limitedReader wraps an io.Reader and returns an error after reading a certain number of bytes
type limitedReader struct {
	r     io.Reader
	limit int64
	read  int64
}

func (lr *limitedReader) Read(p []byte) (n int, err error) {
	if lr.read >= lr.limit {
		return 0, io.ErrUnexpectedEOF
	}
	remaining := lr.limit - lr.read
	if int64(len(p)) > remaining {
		p = p[:remaining]
	}
	n, err = lr.r.Read(p)
	lr.read += int64(n)
	return n, err
}

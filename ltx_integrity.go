package litestream

import (
	"errors"
	"fmt"
	"io"

	"github.com/superfly/ltx"
)

// LTX integrity verification errors
var (
	ErrLTXCorrupted       = errors.New("ltx file is corrupted")
	ErrLTXHeaderInvalid   = errors.New("ltx header is invalid")
	ErrLTXPageInvalid     = errors.New("ltx page data is invalid")
	ErrLTXTrailerInvalid  = errors.New("ltx trailer is invalid")
	ErrLTXChecksumInvalid = errors.New("ltx checksum mismatch")
)

// ValidateLTX streams through an LTX file and validates all CRC-64 checksums.
// This uses the built-in ltx.Decoder verification which checks:
// - Header integrity
// - Page data checksums
// - Trailer integrity
//
// This function is designed for integrity verification, not for the hot path.
// Use it in tests, scrubbing jobs, or after restore operations.
func ValidateLTX(r io.Reader) error {
	dec := ltx.NewDecoder(r)

	// Verify decodes the entire file and validates all checksums
	if err := dec.Verify(); err != nil {
		return fmt.Errorf("%w: %v", ErrLTXCorrupted, err)
	}

	return nil
}

// ValidateLTXHeader validates only the header portion of an LTX file.
// Useful for quick validation without reading the entire file.
func ValidateLTXHeader(r io.Reader) (ltx.Header, error) {
	dec := ltx.NewDecoder(r)
	if err := dec.DecodeHeader(); err != nil {
		return ltx.Header{}, fmt.Errorf("%w: %v", ErrLTXHeaderInvalid, err)
	}
	return dec.Header(), nil
}

// ValidateLTXWithCallback validates an LTX file and calls the callback for each page.
// This allows for custom validation logic during streaming.
func ValidateLTXWithCallback(r io.Reader, onPage func(pgno uint32, data []byte) error) error {
	dec := ltx.NewDecoder(r)

	if err := dec.DecodeHeader(); err != nil {
		return fmt.Errorf("%w: %v", ErrLTXHeaderInvalid, err)
	}

	header := dec.Header()
	pageSize := header.PageSize
	buf := make([]byte, pageSize)

	for {
		var hdr ltx.PageHeader
		if err := dec.DecodePage(&hdr, buf); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return fmt.Errorf("%w: %v", ErrLTXPageInvalid, err)
		}

		if onPage != nil {
			if err := onPage(hdr.Pgno, buf); err != nil {
				return err
			}
		}
	}

	return nil
}

// LTXIntegrityReport contains detailed information about LTX file validation.
type LTXIntegrityReport struct {
	Valid      bool
	Header     ltx.Header
	PageCount  int
	TotalBytes int64
	Error      error
}

// ValidateLTXWithReport validates an LTX file and returns a detailed report.
func ValidateLTXWithReport(r io.Reader) *LTXIntegrityReport {
	report := &LTXIntegrityReport{Valid: true}

	dec := ltx.NewDecoder(r)

	if err := dec.DecodeHeader(); err != nil {
		report.Valid = false
		report.Error = fmt.Errorf("%w: %v", ErrLTXHeaderInvalid, err)
		return report
	}

	report.Header = dec.Header()
	pageSize := report.Header.PageSize
	buf := make([]byte, pageSize)
	report.TotalBytes = int64(ltx.HeaderSize)

	for {
		var hdr ltx.PageHeader
		if err := dec.DecodePage(&hdr, buf); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			report.Valid = false
			report.Error = fmt.Errorf("%w at page %d: %v", ErrLTXPageInvalid, report.PageCount, err)
			return report
		}
		report.PageCount++
		report.TotalBytes += int64(ltx.PageHeaderSize) + int64(pageSize)
	}

	return report
}

package main

import (
	"encoding/csv"
	"fmt"
	"os"
)

// ouvre un csv et retourne un reader et le fichier ouvert
func openCSV(path string) (*csv.Reader, *os.File, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, fmt.Errorf("open %s: %w", path, err)
	}
	r := csv.NewReader(f)
	r.LazyQuotes = true
	r.FieldsPerRecord = -1 // ça évite de bloquer sur une ligne bancale
	return r, f, nil
}

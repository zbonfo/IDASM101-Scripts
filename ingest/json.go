package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// lit un tableau json entrée par entrée
// ça évite de charger le fichier complet en mémoire
func streamJSON[T any](path string, errLog *ErrorLogger, fn func(T)) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open %s: %w", path, err)
	}
	defer f.Close()

	r := bufio.NewReaderSize(f, 256*1024) // buffer de 256kb

	b, err := nextByte(r)
	if err != nil {
		return fmt.Errorf("lecture du début de %s: %w", path, err)
	}
	if b != '[' {
		return fmt.Errorf("attendu '[' au début de %s, trouvé %q", path, b)
	}

	for {
		b, err = nextByte(r)
		if err == io.EOF {
			return fmt.Errorf("fin de fichier trop tôt dans %s", path)
		}
		if err != nil {
			return fmt.Errorf("lecture token dans %s: %w", path, err)
		}

		switch b {
		case ']':
			return nil
		case ',':
			continue
		case '{':
			raw, err := readObj(r, b)
			if err != nil {
				return fmt.Errorf("lecture objet dans %s: %w", path, err)
			}
			var item T
			if err := json.Unmarshal(raw, &item); err != nil {
				errLog.Log(filepath.Base(path), string(raw), fmt.Sprintf("json: %v", err))
				continue
			}
			fn(item)
		default:
			return fmt.Errorf("token inattendu %q dans %s", b, path)
		}
	}
}

func nextByte(r *bufio.Reader) (byte, error) {
	for {
		b, err := r.ReadByte()
		if err != nil {
			return 0, err
		}
		if b == ' ' || b == '\n' || b == '\r' || b == '\t' {
			continue
		}
		return b, nil
	}
}

func readObj(r *bufio.Reader, first byte) ([]byte, error) {
	var raw []byte
	raw = append(raw, first)
	depth := 1
	inString := false
	escaped := false

	for depth > 0 {
		b, err := r.ReadByte()
		if err != nil {
			return nil, err
		}
		raw = append(raw, b)

		if inString {
			if escaped {
				escaped = false
				continue
			}
			if b == '\\' {
				escaped = true
				continue
			}
			if b == '"' {
				inString = false
			}
			continue
		}

		switch b {
		case '"':
			inString = true
		case '{':
			depth++
		case '}':
			depth--
		}
	}

	return raw, nil
}

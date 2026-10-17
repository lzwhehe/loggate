// Command piigen writes a deterministic synthetic PII dataset (JSONL, one
// record per line with byte-offset span labels and recorded hard
// negatives). All values are generated from the seed; no real personal
// data is involved. See internal/piibench.
//
//	piigen -seed 20261017 -n 10000 -out pii.jsonl
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/lzwhehe/loggate/internal/piibench"
)

func main() {
	seed := flag.Uint64("seed", 20261017, "generator seed")
	n := flag.Int("n", 10000, "number of records")
	out := flag.String("out", "pii.jsonl", "output JSONL path ('-' = stdout)")
	flag.Parse()
	if err := run(*seed, *n, *out); err != nil {
		fmt.Fprintln(os.Stderr, "piigen:", err)
		os.Exit(1)
	}
}

func run(seed uint64, n int, out string) error {
	f := os.Stdout
	if out != "-" {
		var err error
		if f, err = os.Create(out); err != nil {
			return err
		}
		defer f.Close()
	}
	w := bufio.NewWriter(f)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	for _, r := range piibench.Generate(seed, n) {
		if err := enc.Encode(r); err != nil {
			return err
		}
	}
	if err := w.Flush(); err != nil {
		return err
	}
	if out != "-" {
		fmt.Fprintf(os.Stderr, "piigen: wrote %d records (seed %d) to %s\n", n, seed, out)
	}
	return nil
}

// Reads what each run of scripts/spike.sh left behind, checks every run against the counts taken outside the system, and summarises each scenario with median and range.
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "spikereport: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	dir := flag.String("dir", "data/spike", "directory holding the files scripts/spike.sh wrote")
	out := flag.String("out", "data/spike-summary.json", "path for the JSON summary; empty writes none")
	flag.Parse()

	runs, err := loadRuns(*dir)
	if err != nil {
		return err
	}
	rep := summarize(runs)
	printSummary(rep)

	if *out != "" {
		b, err := json.MarshalIndent(rep, "", "  ")
		if err != nil {
			return fmt.Errorf("encoding summary: %w", err)
		}
		if err := os.WriteFile(*out, append(b, '\n'), 0o644); err != nil {
			return fmt.Errorf("writing %s: %w", *out, err)
		}
		fmt.Printf("\nsummary %s\n", *out)
	}
	if rep.RunsFailed > 0 {
		return fmt.Errorf("%d of %d runs failed at least one check", rep.RunsFailed, rep.Runs)
	}
	return nil
}

func loadRuns(dir string) ([]runFiles, error) {
	paths, err := filepath.Glob(filepath.Join(dir, "*-checks.json"))
	if err != nil {
		return nil, fmt.Errorf("listing %s: %w", dir, err)
	}
	if len(paths) == 0 {
		return nil, fmt.Errorf("no *-checks.json files in %s; run scripts/spike.sh first", dir)
	}
	sort.Strings(paths)

	var runs []runFiles
	var problems []error
	for _, path := range paths {
		base := strings.TrimSuffix(path, "-checks.json")
		var r runFiles
		if err := load(path, &r.Checks, checksKeys); err != nil {
			problems = append(problems, err)
			continue
		}
		if err := load(base+"-producer.json", &r.Producer, producerKeys); err != nil {
			problems = append(problems, err)
			continue
		}
		if err := load(base+"-consumer.json", &r.Consumer, consumerKeys); err != nil {
			problems = append(problems, err)
			continue
		}
		if _, err := os.Stat(base + "-load.json"); err == nil {
			r.Load = &loadReport{}
			if err := load(base+"-load.json", r.Load, loadKeys); err != nil {
				problems = append(problems, err)
				continue
			}
		}
		runs = append(runs, r)
	}
	if len(problems) > 0 {
		return nil, errors.Join(problems...)
	}
	return runs, nil
}

func load(path string, into any, required []string) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("reading %s: %w", path, err)
	}
	if err := requireKeys(b, required); err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	if err := json.Unmarshal(b, into); err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	return nil
}

func requireKeys(doc []byte, paths []string) error {
	var root map[string]json.RawMessage
	if err := json.Unmarshal(doc, &root); err != nil {
		return fmt.Errorf("not a JSON object: %w", err)
	}

	var problems []error
	for _, path := range paths {
		node := root
		parts := strings.Split(path, ".")
		for i, part := range parts {
			raw, ok := node[part]
			if !ok {
				problems = append(problems, fmt.Errorf("key %q is missing", path))
				break
			}
			if i == len(parts)-1 {
				break
			}
			var next map[string]json.RawMessage
			if err := json.Unmarshal(raw, &next); err != nil {
				problems = append(problems, fmt.Errorf("key %q: %q is not an object", path, part))
				break
			}
			node = next
		}
	}
	return errors.Join(problems...)
}

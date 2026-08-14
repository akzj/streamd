package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/akzj/streamd/internal/storage/scrub"
	"github.com/akzj/streamd/internal/storage/snapshot"
)

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	var result any
	var err error
	switch os.Args[1] {
	case "scrub":
		flags := flag.NewFlagSet("scrub", flag.ExitOnError)
		data := flags.String("data", "", "offline streamd data directory")
		_ = flags.Parse(os.Args[2:])
		if *data == "" {
			usage()
		}
		result, err = scrub.DataRoot(*data)
	case "snapshot":
		flags := flag.NewFlagSet("snapshot", flag.ExitOnError)
		data := flags.String("data", "", "offline streamd data directory")
		out := flags.String("out", "", "new Snapshot directory")
		_ = flags.Parse(os.Args[2:])
		if *data == "" || *out == "" {
			usage()
		}
		result, err = snapshot.Create(*data, *out)
	case "verify-snapshot":
		flags := flag.NewFlagSet("verify-snapshot", flag.ExitOnError)
		path := flags.String("path", "", "Snapshot directory")
		_ = flags.Parse(os.Args[2:])
		if *path == "" {
			usage()
		}
		result, err = snapshot.Verify(*path)
	default:
		usage()
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "streamd-tool:", err)
		os.Exit(1)
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err = encoder.Encode(result); err != nil {
		fmt.Fprintln(os.Stderr, "streamd-tool:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: streamd-tool scrub -data DIR | snapshot -data DIR -out DIR | verify-snapshot -path DIR")
	os.Exit(2)
}

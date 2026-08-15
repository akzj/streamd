package main

import (
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/akzj/streamd/internal/storage/format"
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
	case "install-snapshot":
		flags := flag.NewFlagSet("install-snapshot", flag.ExitOnError)
		data := flags.String("data", "", "offline target streamd data directory")
		path := flags.String("path", "", "verified Snapshot directory")
		term := flags.Uint64("term", 0, "current Coordinator Term")
		leader := flags.String("leader-id", "", "current Leader UUID")
		_ = flags.Parse(os.Args[2:])
		leaderID, parseErr := parseUUID(*leader)
		if *data == "" || *path == "" || *term == 0 || parseErr != nil {
			usage()
		}
		result, err = snapshot.Install(*data, *path, snapshot.InstallOptions{Term: *term, LeaderID: leaderID})
	case "resume-install":
		flags := flag.NewFlagSet("resume-install", flag.ExitOnError)
		data := flags.String("data", "", "offline target streamd data directory")
		_ = flags.Parse(os.Args[2:])
		if *data == "" {
			usage()
		}
		var resumed bool
		resumed, err = snapshot.ResumeInstall(*data, nil)
		result = map[string]bool{"resumed": resumed}
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
	fmt.Fprintln(os.Stderr, "usage: streamd-tool scrub -data DIR | snapshot -data DIR -out DIR | verify-snapshot -path DIR | install-snapshot -data DIR -path DIR -term N -leader-id UUID | resume-install -data DIR")
	os.Exit(2)
}

func parseUUID(value string) (format.UUID, error) {
	var id format.UUID
	compact := strings.ReplaceAll(value, "-", "")
	if len(compact) != 32 {
		return id, fmt.Errorf("UUID must contain 32 hexadecimal digits")
	}
	decoded, err := hex.DecodeString(compact)
	if err != nil {
		return id, err
	}
	copy(id[:], decoded)
	if id == (format.UUID{}) {
		return id, fmt.Errorf("UUID is zero")
	}
	return id, nil
}

package main

import (
	"fmt"
	"os"

	"github.com/tandemn-labs/tandemn-efficiency-index/internal/cli"
)

func main() {
	if err := cli.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

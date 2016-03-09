package main

import (
	"github.com/ngaut/tidb-experimental/store/cockroach"
	"github.com/pingcap/tidb"
)

func init() {
	// Reister cockroach store
	tidb.RegisterStore("cockroach", cockroach.Driver{})
}

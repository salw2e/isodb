package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"runtime/pprof"
	"syscall"

	"isodb/db"
	"isodb/wire"
)

func main() {
	addr := flag.String("listen", "127.0.0.1:15433", "address to listen on")
	dir := flag.String("data", "data", "data directory, empty for memory only")
	flag.Parse()
	if path := os.Getenv("ISODB_CPUPROFILE"); path != "" {
		f, err := os.Create(path)
		if err == nil {
			pprof.StartCPUProfile(f)
			defer pprof.StopCPUProfile()
		}
	}
	d, err := db.Open(*dir)
	if err != nil {
		fmt.Fprintln(os.Stderr, "isodb:", err)
		os.Exit(1)
	}
	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		fmt.Fprintln(os.Stderr, "isodb:", err)
		os.Exit(1)
	}
	fmt.Println("isodb listening on", *addr)
	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
		<-sig
		ln.Close()
		err := d.Close()
		pprof.StopCPUProfile()
		if err != nil {
			fmt.Fprintln(os.Stderr, "isodb:", err)
			os.Exit(1)
		}
		os.Exit(0)
	}()
	wire.Serve(ln, d)
	select {}
}

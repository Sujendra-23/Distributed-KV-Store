// Command client is a small CLI for exercising the cluster:
//
//	client -coordinator localhost:8080 put foo bar
//	client -coordinator localhost:8080 get foo
//	client -coordinator localhost:8080 delete foo
//	client -coordinator localhost:8080 status
package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
)

func main() {
	coordinator := flag.String("coordinator", "localhost:8080", "coordinator HTTP address")
	flag.Parse()
	args := flag.Args()
	if len(args) < 1 {
		fmt.Println("usage: client [-coordinator addr] <put|get|delete|status> [key] [value]")
		os.Exit(1)
	}

	base := "http://" + *coordinator
	switch args[0] {
	case "put":
		if len(args) != 3 {
			fatal("usage: client put <key> <value>")
		}
		req, _ := http.NewRequest(http.MethodPut, base+"/kv/"+args[1], bytes.NewBufferString(args[2]))
		resp, err := http.DefaultClient.Do(req)
		check(err)
		defer resp.Body.Close()
		fmt.Println(resp.Status)

	case "get":
		if len(args) != 2 {
			fatal("usage: client get <key>")
		}
		resp, err := http.Get(base + "/kv/" + args[1])
		check(err)
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			fmt.Println(resp.Status)
			return
		}
		body, _ := io.ReadAll(resp.Body)
		fmt.Println(string(body))

	case "delete":
		if len(args) != 2 {
			fatal("usage: client delete <key>")
		}
		req, _ := http.NewRequest(http.MethodDelete, base+"/kv/"+args[1], nil)
		resp, err := http.DefaultClient.Do(req)
		check(err)
		defer resp.Body.Close()
		fmt.Println(resp.Status)

	case "status":
		resp, err := http.Get(base + "/status")
		check(err)
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		fmt.Println(string(body))

	default:
		fatal("unknown command: " + args[0])
	}
}

func check(err error) {
	if err != nil {
		fatal(err.Error())
	}
}

func fatal(msg string) {
	fmt.Fprintln(os.Stderr, msg)
	os.Exit(1)
}

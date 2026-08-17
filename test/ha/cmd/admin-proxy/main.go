package main

import (
	"flag"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"time"
)

func main() {
	listen := flag.String("listen", "0.0.0.0:19090", "proxy listen address")
	targetValue := flag.String("target", "http://127.0.0.1:9090", "loopback admin target")
	flag.Parse()
	target, err := url.Parse(*targetValue)
	if err != nil || target.Scheme != "http" || target.Host == "" {
		log.Fatalf("invalid target %q", *targetValue)
	}
	proxy := httputil.NewSingleHostReverseProxy(target)
	server := &http.Server{Addr: *listen, Handler: proxy, ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 30 * time.Second}
	log.Fatal(server.ListenAndServe())
}

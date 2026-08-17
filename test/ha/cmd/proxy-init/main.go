package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"time"
)

type proxy struct {
	Name     string `json:"name"`
	Listen   string `json:"listen"`
	Upstream string `json:"upstream"`
	Enabled  bool   `json:"enabled"`
}

func main() {
	endpoint := env("TOXIPROXY_API", "http://toxiproxy:8474")
	proxies := []proxy{
		{Name: "etcd-primary-1", Listen: "0.0.0.0:12379", Upstream: "etcd-1:2379", Enabled: true},
		{Name: "etcd-primary-2", Listen: "0.0.0.0:12380", Upstream: "etcd-2:2379", Enabled: true},
		{Name: "etcd-primary-3", Listen: "0.0.0.0:12381", Upstream: "etcd-3:2379", Enabled: true},
		{Name: "etcd-standby-1", Listen: "0.0.0.0:22379", Upstream: "etcd-1:2379", Enabled: true},
		{Name: "etcd-standby-2", Listen: "0.0.0.0:22380", Upstream: "etcd-2:2379", Enabled: true},
		{Name: "etcd-standby-3", Listen: "0.0.0.0:22381", Upstream: "etcd-3:2379", Enabled: true},
		{Name: "standby", Listen: "0.0.0.0:17443", Upstream: "streamd-standby:7443", Enabled: true},
		{Name: "former-primary", Listen: "0.0.0.0:27443", Upstream: "streamd-primary:7443", Enabled: true},
	}
	client := &http.Client{Timeout: 2 * time.Second}
	deadline := time.Now().Add(30 * time.Second)
	for {
		if err := createAll(client, endpoint, proxies); err == nil {
			return
		} else if time.Now().After(deadline) {
			fatal(err)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

func createAll(client *http.Client, endpoint string, proxies []proxy) error {
	for _, value := range proxies {
		encoded, err := json.Marshal(value)
		if err != nil {
			return err
		}
		response, err := client.Post(endpoint+"/proxies", "application/json", bytes.NewReader(encoded))
		if err != nil {
			return err
		}
		response.Body.Close()
		if response.StatusCode != http.StatusCreated && response.StatusCode != http.StatusConflict {
			return fmt.Errorf("create proxy %s: %s", value.Name, response.Status)
		}
	}
	return nil
}

func env(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "initialize Toxiproxy:", err)
	os.Exit(1)
}

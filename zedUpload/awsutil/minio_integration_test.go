//go:build integration
// +build integration

// Copyright(c) 2025 Zededa,
// All rights reserved.

package awsutil

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/joho/godotenv"
	tc "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

var minioC tc.Container // will be non-nil when we start a container

func getenvDefault(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func TestMain(m *testing.M) {
	// Load .env if present (works for both external or container modes)
	_ = godotenv.Load() // optionally: godotenv.Load(".env.test")

	ctx := context.Background()

	fmt.Println("Starting tests")

	// Decide whether to start a container
	endpoint := os.Getenv("MINIO_ENDPOINT")

	user := getenvDefault("MINIO_ACCESS_KEY_ID", "minioadmin")
	pass := getenvDefault("MINIO_SECRET_ACCESS_KEY", "minioadmin")

	req := tc.ContainerRequest{
		Image:        "minio/minio:latest",
		ExposedPorts: []string{"9000/tcp", "9001/tcp"},
		Env: map[string]string{
			"MINIO_ROOT_USER":     user,
			"MINIO_ROOT_PASSWORD": pass,
		},
		Cmd:        []string{"server", "/data", "--console-address", ":9001"},
		WaitingFor: wait.ForHTTP("/minio/health/live").WithPort("9000/tcp").WithStartupTimeout(60 * time.Second),
	}

	c, err := tc.GenericContainer(ctx, tc.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		log.Fatalf("failed to start MinIO container: %v", err)
	}
	minioC = c

	host, err := c.Host(ctx)
	if err != nil {
		log.Fatalf("failed to get container host: %v", err)
	}
	apiPort, err := c.MappedPort(ctx, "9000/tcp")
	if err != nil {
		log.Fatalf("failed to get mapped API port: %v", err)
	}
	endpoint = fmt.Sprintf("http://%s:%s", host, apiPort.Port())

	if err := waitReady(endpoint); err != nil {
		log.Fatalf("minio not yet ready: %v", err)
	}

	// Export env for tests (overriding anything from .env)
	os.Setenv("MINIO_ENDPOINT", endpoint)
	os.Setenv("MINIO_ACCESS_KEY_ID", user)
	os.Setenv("MINIO_SECRET_ACCESS_KEY", pass)
	os.Setenv("MINIO_REGION", getenvDefault("MINIO_REGION", "us-east-1"))

	// Ensure we always have a unique test bucket if one isn't provided.
	if os.Getenv("MINIO_TEST_BUCKET") == "" {
		base := getenvDefault("MINIO_TEST_BUCKET_BASE", "it-bucket")
		os.Setenv("MINIO_TEST_BUCKET", fmt.Sprintf("%s-%d", base, time.Now().UnixNano()))
	}

	// Run tests
	code := m.Run()

	// Teardown container if we started one
	if minioC != nil {
		_ = minioC.Terminate(ctx)
	}
	os.Exit(code)
}

func waitReady(endpoint string) error {
	client := &http.Client{Timeout: 2 * time.Second}
	readyURL := endpoint + "/minio/health/ready"
	deadline := time.Now().Add(30 * time.Second)
	for {
		resp, err := client.Get(readyURL)
		if err == nil && resp.StatusCode == 200 {
			_ = resp.Body.Close()
			return nil
		}
		if time.Now().After(deadline) {
			if err != nil {
				return fmt.Errorf("timeout waiting for %s: %w", readyURL, err)
			}
			if resp != nil {
				_ = resp.Body.Close()
				return fmt.Errorf("timeout: %s got HTTP %d", readyURL, resp.StatusCode)
			}
			return fmt.Errorf("timeout: %s not reachable", readyURL)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

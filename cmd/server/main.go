// Command server is the DevOops training backend.
//
// It accepts job.yaml (JobSet) + train.py + data from the frontend, uploads the inputs to S3,
// and submits the prepared JobSet to the local k3s management cluster, where MultiKueue
// dispatches it to whichever EKS worker admits it first.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/DevOops-Team/Backend/internal/api"
	"github.com/DevOops-Team/Backend/internal/kube"
	"github.com/DevOops-Team/Backend/internal/store"
)

func main() {
	if err := run(); err != nil {
		slog.Error("server stopped", "err", err)
		os.Exit(1)
	}
}

func run() error {
	cfg := api.Config{
		Namespace:     env("TRAINING_NAMESPACE", "training"),
		QueueName:     env("QUEUE_NAME", "queue-auto"),
		ProgressTable: env("PROGRESS_TABLE", "devoops-job-progress"),
		AllowedOrigin: env("ALLOWED_ORIGIN", "http://localhost:5173"),
		NewID:         newID,
	}
	bucket := mustEnv("S3_BUCKET")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	awsCfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(env("AWS_REGION", "us-east-1")))
	if err != nil {
		return err
	}
	k8s, err := kube.New(os.Getenv("KUBECONFIG"), os.Getenv("KUBE_CONTEXT"), cfg.Namespace)
	if err != nil {
		return err
	}

	srv := &http.Server{
		Addr: env("LISTEN_ADDR", ":8080"),
		Handler: api.NewServer(cfg,
			store.NewObjects(s3.NewFromConfig(awsCfg), bucket),
			k8s,
			store.NewProgress(dynamodb.NewFromConfig(awsCfg), cfg.ProgressTable),
		),
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		srv.Shutdown(shutdown)
	}()

	slog.Info("listening", "addr", srv.Addr, "bucket", bucket, "namespace", cfg.Namespace, "queue", cfg.QueueName)
	if err := srv.ListenAndServe(); err != http.ErrServerClosed {
		return err
	}
	return nil
}

// newID returns 8 lowercase hex chars: short, and valid in Kubernetes names and S3 keys.
func newID() string {
	b := make([]byte, 4)
	rand.Read(b)
	return hex.EncodeToString(b)
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		slog.Error("missing required env", "key", key)
		os.Exit(2)
	}
	return v
}

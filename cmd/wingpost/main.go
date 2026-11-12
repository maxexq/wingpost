// Command wingpost runs the ingest API, the delivery worker, and the admin
// commands that set up apps and endpoints.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/maxexq/wingpost/internal/core"
	"github.com/maxexq/wingpost/internal/dispatch"
	"github.com/maxexq/wingpost/internal/ingest"
	"github.com/maxexq/wingpost/internal/postgres"
)

const usage = `usage: wingpost <command> [flags]

commands:
  migrate         create the database tables
  api             run the ingest API
  worker          run the delivery worker
  app-create      create an app and print its API key
  endpoint-add    add a destination to an app

environment:
  DATABASE_URL    PostgreSQL connection URL (required)
  ADDR            listen address of the API (default ":8080")
`

func main() {
	if len(os.Args) < 2 {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))

	if err := run(ctx, log, os.Args[1], os.Args[2:]); err != nil {
		fmt.Fprintln(os.Stderr, "wingpost:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, log *slog.Logger, command string, args []string) error {
	if command == "help" || command == "-h" || command == "--help" {
		fmt.Print(usage)
		return nil
	}
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		return errors.New("DATABASE_URL is not set")
	}
	store, err := postgres.Open(ctx, url)
	if err != nil {
		return err
	}
	defer store.Close()

	switch command {
	case "migrate":
		return store.Migrate(ctx)
	case "api":
		return runAPI(ctx, log, store)
	case "worker":
		return runWorker(ctx, log, store, args)
	case "app-create":
		return appCreate(ctx, store, args)
	case "endpoint-add":
		return endpointAdd(ctx, store, args)
	default:
		return fmt.Errorf("unknown command %q\n\n%s", command, usage)
	}
}

func runAPI(ctx context.Context, log *slog.Logger, store *postgres.Store) error {
	addr := os.Getenv("ADDR")
	if addr == "" {
		addr = ":8080"
	}
	handler := &ingest.Handler{Store: store, Now: time.Now, Log: log}
	server := &http.Server{
		Addr:              addr,
		Handler:           handler.Routes(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()
	log.Info("api listening", "addr", addr)
	if err := server.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func runWorker(ctx context.Context, log *slog.Logger, store *postgres.Store, args []string) error {
	flags := flag.NewFlagSet("worker", flag.ContinueOnError)
	interval := flags.Duration("interval", time.Second, "wait between polls when the queue is empty")
	batch := flags.Int("batch", 20, "deliveries to claim in one poll")
	timeout := flags.Duration("timeout", 10*time.Second, "time limit for one delivery attempt")
	if err := flags.Parse(args); err != nil {
		return err
	}
	worker := &dispatch.Worker{
		Queue:    store,
		Sender:   &dispatch.HTTPSender{Client: &http.Client{Timeout: *timeout}},
		Schedule: core.DefaultSchedule,
		Now:      time.Now,
		// One batch runs in sequence, so the lease must cover a full batch
		// of attempts that all time out.
		Lease: time.Duration(*batch)**timeout + time.Minute,
		Batch: *batch,
		Log:   log,
	}
	log.Info("worker started", "interval", interval.String(), "batch", *batch)
	return worker.Run(ctx, *interval)
}

func appCreate(ctx context.Context, store *postgres.Store, args []string) error {
	flags := flag.NewFlagSet("app-create", flag.ContinueOnError)
	name := flags.String("name", "", "name of the app (required)")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *name == "" {
		return errors.New("app-create: -name is required")
	}
	key, err := randomToken("wp_")
	if err != nil {
		return err
	}
	app, err := store.CreateApp(ctx, *name, core.HashKey(key))
	if err != nil {
		return err
	}
	fmt.Printf("app_id:  %s\napi_key: %s\n", app.ID, key)
	fmt.Println("Wingpost stores only a hash of the key. Save the key now.")
	return nil
}

func endpointAdd(ctx context.Context, store *postgres.Store, args []string) error {
	flags := flag.NewFlagSet("endpoint-add", flag.ContinueOnError)
	appID := flags.String("app", "", "app id (required)")
	url := flags.String("url", "", "destination URL (required)")
	events := flags.String("events", "*", `comma-separated event patterns, for example "home.*,site.down"`)
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *appID == "" || *url == "" {
		return errors.New("endpoint-add: -app and -url are required")
	}
	secret, err := randomToken("whsec_")
	if err != nil {
		return err
	}
	ep, err := store.CreateEndpoint(ctx, core.Endpoint{
		AppID:    *appID,
		URL:      *url,
		Secret:   secret,
		Patterns: strings.Split(*events, ","),
	})
	if err != nil {
		return err
	}
	fmt.Printf("endpoint_id:    %s\nsigning_secret: %s\n", ep.ID, secret)
	return nil
}

func randomToken(prefix string) (string, error) {
	raw := make([]byte, 24)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	return prefix + hex.EncodeToString(raw), nil
}

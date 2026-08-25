package observability

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Start exposes internal Prometheus metrics on a dedicated listener. Listener
// failures are logged and never terminate the product service.
func Start(ctx context.Context, addr, service string, pool *pgxpool.Pool) {
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		writePoolMetrics(w, service, pool)
	})

	server := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			log.Printf("metrics shutdown: %v", err)
		}
	}()

	go func() {
		log.Printf("Metrics listener started on %s", addr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("metrics listener error: %v", err)
		}
	}()
}

func writePoolMetrics(w http.ResponseWriter, service string, pool *pgxpool.Pool) {
	stat := pool.Stat()
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	_, _ = fmt.Fprintf(w, `# HELP sambacrm_db_pool_max_connections Configured maximum database pool size.
# TYPE sambacrm_db_pool_max_connections gauge
sambacrm_db_pool_max_connections{service=%q} %d
# HELP sambacrm_db_pool_connections Current database connections in the pool.
# TYPE sambacrm_db_pool_connections gauge
sambacrm_db_pool_connections{service=%q} %d
# HELP sambacrm_db_pool_idle_connections Current idle database connections in the pool.
# TYPE sambacrm_db_pool_idle_connections gauge
sambacrm_db_pool_idle_connections{service=%q} %d
# HELP sambacrm_db_pool_in_use_connections Current acquired database connections in the pool.
# TYPE sambacrm_db_pool_in_use_connections gauge
sambacrm_db_pool_in_use_connections{service=%q} %d
`, service, stat.MaxConns(), service, stat.TotalConns(), service, stat.IdleConns(), service, stat.AcquiredConns())
}

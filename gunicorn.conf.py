"""Gunicorn configuration for production deployment on Railway."""
import os

# Binding
bind = f"0.0.0.0:{os.environ.get('PORT', '8080')}"

# Workers: single worker required — app uses in-memory job state (_jobs dict).
# Multiple workers would each have their own _jobs, breaking job tracking.
workers = 1

# Use gevent async worker: handles SSE connections without blocking threads.
# Each SSE stream uses a lightweight greenlet instead of a full OS thread,
# so hundreds of concurrent viewers won't starve regular HTTP requests.
worker_class = "gevent"
worker_connections = 200

# Timeouts
timeout = 300          # Allow long-running SSE connections (5 min)
graceful_timeout = 30  # Wait for active requests on shutdown
keepalive = 5

# Logging
accesslog = "-"  # stdout
errorlog = "-"   # stderr
loglevel = "info"

# Security limits
limit_request_line = 4094
limit_request_fields = 50
limit_request_field_size = 8190

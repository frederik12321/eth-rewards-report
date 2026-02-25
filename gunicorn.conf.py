"""Gunicorn configuration for production deployment on Railway."""
import os

# Binding
bind = f"0.0.0.0:{os.environ.get('PORT', '8080')}"

# Workers: single worker required — app uses in-memory job state (_jobs dict).
# Multiple workers would each have their own _jobs, breaking job tracking.
workers = 1

# Threads: handle concurrent requests within the single worker.
threads = 4

# Timeouts
timeout = 120          # Allow long-running SSE connections
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

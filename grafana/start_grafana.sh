#!/bin/bash

# Adjust paths here if necessary
GRAFANA_DIR="/opt/homebrew/share/grafana"
PROMETHEUS_CONFIG="/opt/homebrew/etc/prometheus.yml"
PROJECT_DIR="$HOME/Documents/GitHub/styx_demo"

# Copy provisioning files
cp -r "provisioning" "$GRAFANA_DIR"
mkdir -p "$GRAFANA_DIR/dashboards"
cp "$PROJECT_DIR/dashboards/"*.json "$GRAFANA_DIR/dashboards/"

# Start Prometheus
echo "Starting Prometheus..."
prometheus --config.file="$PROMETHEUS_CONFIG" --web.enable-lifecycle > /tmp/prometheus.log 2>&1 &

# Wait a bit to ensure Prometheus is up
sleep 5

# Start Grafana
echo "Starting Grafana..."
grafana-server --config /opt/homebrew/etc/grafana/grafana.ini --homepath "$GRAFANA_DIR"

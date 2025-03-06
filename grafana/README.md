# Link and you missed it! - Styx Demo

In this demo, we showcase how [Styx](https://arxiv.org/pdf/2312.06893) works in practice. In three scenarios, we present how Styx is easy to setup, easy to deploy & reconfigure, and how it can handle and recover from failures efficiently. To demonstrate these favorable properties in practice, we develop a Grafana Dashboard, displaying the key performance metrics of Styx.

## Installation guide

 - Install Grafana with homebrew `brew install grafana`
 - Update `/opt/homebrew/etc/grafana/grafana.ini` for the lines
 ```
 # The ip address to bind to, empty will bind to all interfaces
http_addr = 0.0.0.0  # Allow connections from any host

# The http port to use
http_port = 3000  # Ensure Grafana uses port 3000
 ```
 - Allow embeddings in the grafana.ini `allow_embedding = true`
 - Update the provisoning path `provisioning = /opt/homebrew/share/grafana/provisioning`
 - Install dependencies for webpage serving `npm init -y` and `npm install express`
 - (Note: [4.3.2025] the list of updates to grafana.ini may be incomplete.)

## Setup Metric collection with Prometheus

Setup Prometheus to expose metrics for monitoring:

1. Install Prometheus `brew install prometheus node_exporter`
2. Make sure Prometheus scrapes from all the sources. `/opt/homebrew/etc/prometheus.yml` should look like this:
```
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: "prometheus"
    static_configs:
      - targets: ["localhost:9090"]
  - job_name: "node_exporter"
    static_configs:
      - targets: ["localhost:9100"]
  - job_name: "my_python_app"
    static_configs:
      - targets: ["localhost:8000"]
```
3. Start node exporter `brew services start node_exporter` (It will run on `http://localhost:9100/metrics`)
4. Start prometheus `brew services start prometheus` (It will run on `http://localhost:9090/query`)

Instructions for shut down
1. Stop the node exporter `brew services stop node_exporter`
2. Stop the prometheus `brew services stop prometheus`

## Running instructions (manually)

1. Start Grafana `grafana server --config /opt/homebrew/etc/grafana/grafana.ini --homepath /opt/homebrew/opt/grafana/share/grafana`
2. (Optionaly, if you want to embend the Grafana dashboard within a website. We don't do this for the demo) In a separate window, start the webpage serving `python3 -m http.server 8000`

## Running instructions (with script)

Our Bash script will copy over the current versions of the JSON code to generate the dashboards into Grafana's provisioning directory

`bash start_grafana.sh`

## Start dummy custom Prometheus metrics serving

1. Install requirements (if needed) `pip3 install -r requirements.txt`
2. Run the script `python3 styx_backend/dummy_backend.py` (It will run on `http://localhost:8000/metrics`)


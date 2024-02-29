helm repo add bitnami https://charts.bitnami.com/bitnami

helm uninstall kafka
helm uninstall minio

helm install -f helm-config/kafka-config.yml kafka bitnami/kafka
helm install -f helm-config/minio-config.yml minio bitnami/minio
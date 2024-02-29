helm repo add bitnami https://charts.bitnami.com/bitnami
helm repo add ingress-nginx https://kubernetes.github.io/ingress-nginx

helm repo update

helm uninstall kafka
helm uninstall minio
helm uninstall ingress

helm install -f helm-config/kafka-config.yml kafka bitnami/kafka
helm install -f helm-config/minio-config.yml minio bitnami/minio
helm install ingress ingress-nginx/ingress-nginx
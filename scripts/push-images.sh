docker tag dev/styx-coordinator:latest kpsarakis/styx-coordinator:latest
docker push kpsarakis/styx-coordinator:latest

docker tag dev/styx:latest kpsarakis/styx-worker:latest
docker push kpsarakis/styx-worker:latest

docker tag dev/styx-benchmark-client:latest kpsarakis/styx-benchmark-client:latest
docker push kpsarakis/styx-benchmark-client:latest
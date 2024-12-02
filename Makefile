DOCKER_NETWORK = hadoop-network
ENV_FILE = hadoop.env
VERSION_REPOSITORY_DOCKER = 0.0.3

current_branch := $(shell git rev-parse --abbrev-ref HEAD 2>/dev/null || echo "default-branch")

# Realiza criacao da REDE DOCKER_NETWORK
create-network:
	docker network create hadoop_network


build-app-silver-reviews-mongodb:
	docker build -t iamgacarvalho/dmc-app-silver-reviews-mongodb:$(VERSION_REPOSITORY_DOCKER)  ./application/mongodb-processing-historical
	docker push iamgacarvalho/dmc-app-silver-reviews-mongodb:$(VERSION_REPOSITORY_DOCKER)

restart-docker:
	sudo systemctl restart docker

down-services:
	docker rm -f $(docker ps -a -q)


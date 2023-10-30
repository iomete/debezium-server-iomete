docker_image := iomete/debezium-server-iomete
docker_tag := 1.0.0

docker-build:
	# Run this for one time: docker buildx create --use
	docker build -f Dockerfile -t ${docker_image}:${docker_tag} .
	@echo ${docker_image}
	@echo ${docker_tag}

docker-push:
	# Run this for one time: docker buildx create --use
	docker buildx build --platform linux/amd64,linux/arm64 --push -t ${docker_image}:${docker_tag} .
	@echo ${docker_image}
	@echo ${docker_tag}
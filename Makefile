docker_image := iomete.azurecr.io/iomete/debezium-server-iomete
docker_tag := 2.0.0

docker-build:
	# Run this for one time: docker buildx create --use
	docker build -f Dockerfile -t ${docker_image}:${docker_tag} .
	@echo ${docker_image}
	@echo ${docker_tag}

docker-push:
	# Run this for one time: docker buildx create --use
	az acr login --name iomete
	docker buildx build --platform linux/amd64,linux/arm64 --push -f Dockerfile -t ${docker_image}:${docker_tag} . --sbom=true --provenance=true
	az acr repository update --name iomete --image iomete/debezium-server-iomete:${docker_tag} --write-enabled false --delete-enabled false
	@echo ${docker_image}
	@echo ${docker_tag}
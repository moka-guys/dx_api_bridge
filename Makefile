
push: build
	docker push seglh/dx_api_bridge

build:
	docker buildx build --platform linux/amd64 -t seglh/dx_api_bridge .



# Use this to control building and pushing of Docker images in a significantly less ugly fashion.
#

# Top level Makefile commands (also can build individual images with `make build-<hay|etc...>`)
# Image build command definitions
define build-hay-cmd =
docker build -t hay --build-arg go_version=${GO_VERSION} container 
endef

define build-hay-nc-cmd =
docker build --no-cache -t hay --build-arg go_version=${GO_VERSION} container
endef

# Image build command declarations
build-hay:
	$(build-hay-cmd)

build-hay-nc:
	$(build-hay-nc-cmd)

# Image push command definitions
define push-hay-cmd =
	docker tag hay imageregistry.fnal.gov/hay/hay:${HAY_VERSION_TAG}
	docker push imageregistry.fnal.gov/hay/hay:${HAY_VERSION_TAG}
endef

# Image push command declarations
push-hay:
	$(push-hay-cmd)

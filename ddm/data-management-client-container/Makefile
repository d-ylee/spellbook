
# Use this to control building and pushing of Docker images in a significantly less ugly fashion.
#

# Image build command definitions
define build-container-cmd =
podman build --label "maintainer=${USER}" -t ddm-container .
endef

define build-container-nc-cmd =
podman build --label "maintainer=${USER}" --no-cache -t ddm-container .
endef

# Image build command declarations
build-container:
	$(build-container-cmd)

build-container-nc:
	$(build-container-nc-cmd)
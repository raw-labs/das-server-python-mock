.PHONY: fetch build clean

# Location of config.yaml
CONFIG_YAML=config.yaml
# Directory containing helper scripts
SCRIPTS_DIR=scripts
# Directory containing .proto files
PROTO_DIR=proto
# Output directory for generated python stubs
GEN_DIR=.

fetch:
	# Fetch remote .proto files (creates subdirs, etc.)
	mkdir -p $(PROTO_DIR)
	python $(SCRIPTS_DIR)/fetch_protos.py $(CONFIG_YAML) $(PROTO_DIR)

build:
	# Remove existing stubs to avoid stale files
	rm -rf $(GEN_DIR)/com
	# Generate Python stubs from all .proto files
	python -m grpc_tools.protoc \
	    -I $(PROTO_DIR) \
	    --python_out=$(GEN_DIR) \
	    --grpc_python_out=$(GEN_DIR) \
	    $(shell find $(PROTO_DIR) -name '*.proto')
	# Recursively add __init__.py files in generated/ so Python can import them
	python $(SCRIPTS_DIR)/add_init_files.py $(GEN_DIR)/com
	
clean:
	# Removes proto and generated stubs
	rm -rf $(PROTO_DIR)
	rm -rf $(GEN_DIR)/com

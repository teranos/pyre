.PHONY: build install clean test check

PREFIX ?= $(HOME)/.qntx
PLUGIN_NAME := pyre
BINARY_NAME := pyre

build: ## Build pyre via Nix
	@echo "Building $(BINARY_NAME) via Nix..."
	@nix build
	@mkdir -p bin
	@cp -L result/bin/$(BINARY_NAME) bin/
	@chmod +x bin/$(BINARY_NAME)
	@echo "  bin/$(BINARY_NAME) $$(bin/$(BINARY_NAME) --version)"

test: ## Run tests via Nix
	@echo "Running tests..."
	@nix build -L

check: ## Run clippy via Nix
	@echo "Running clippy..."
	@nix build .#checks.$$(nix eval --raw nixpkgs#system).clippy

install: build ## Build and install pyre binary
	@echo "Installing $(BINARY_NAME) to $(PREFIX)/plugins/qntx-$(PLUGIN_NAME)-plugin..."
	@mkdir -p $(PREFIX)/plugins
	@cp bin/$(BINARY_NAME) $(PREFIX)/plugins/qntx-$(PLUGIN_NAME)-plugin
	@chmod +x $(PREFIX)/plugins/qntx-$(PLUGIN_NAME)-plugin
	@echo "  qntx-$(PLUGIN_NAME)-plugin -> $(PREFIX)/plugins/"

clean: ## Clean build artifacts
	@rm -rf bin/ result

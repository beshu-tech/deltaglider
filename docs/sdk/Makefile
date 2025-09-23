# Makefile for DeltaGlider SDK Documentation

.PHONY: all clean generate serve

# Default target
all: generate

# Generate documentation
generate:
	@echo "Generating SDK documentation..."
	python generate_docs.py
	@echo "Documentation generated successfully!"

# Clean generated files
clean:
	@echo "Cleaning generated documentation..."
	rm -f generated_api.md module_index.json
	@echo "Clean complete!"

# Serve documentation locally (requires Python http.server)
serve:
	@echo "Starting documentation server at http://localhost:8000/docs/sdk/"
	cd ../.. && python -m http.server 8000

# Install documentation dependencies
install-deps:
	pip install pdoc3 sphinx sphinx-rtd-theme

# Generate full HTML documentation with pdoc
html:
	pdoc3 --html --output-dir html ../../src/deltaglider

# Generate with Sphinx (future enhancement)
sphinx:
	@echo "Sphinx documentation generation not yet configured"
	@echo "Run 'make install-deps' then 'sphinx-quickstart' to set up"
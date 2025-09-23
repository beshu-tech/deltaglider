# Contributing to DeltaGlider

We love your input! We want to make contributing to DeltaGlider as easy and transparent as possible, whether it's:

- Reporting a bug
- Discussing the current state of the code
- Submitting a fix
- Proposing new features
- Becoming a maintainer

## We Develop with Github

We use GitHub to host code, to track issues and feature requests, as well as accept pull requests.

## We Use [Github Flow](https://guides.github.com/introduction/flow/index.html)

Pull requests are the best way to propose changes to the codebase:

1. Fork the repo and create your branch from `main`.
2. If you've added code that should be tested, add tests.
3. If you've changed APIs, update the documentation.
4. Ensure the test suite passes.
5. Make sure your code lints.
6. Issue that pull request!

## Any contributions you make will be under the MIT Software License

In short, when you submit code changes, your submissions are understood to be under the same [MIT License](LICENSE) that covers the project. Feel free to contact the maintainers if that's a concern.

## Report bugs using Github's [issues](https://github.com/beshu-tech/deltaglider/issues)

We use GitHub issues to track public bugs. Report a bug by [opening a new issue](https://github.com/beshu-tech/deltaglider/issues/new).

**Great Bug Reports** tend to have:

- A quick summary and/or background
- Steps to reproduce
  - Be specific!
  - Give sample code if you can
- What you expected would happen
- What actually happens
- Notes (possibly including why you think this might be happening, or stuff you tried that didn't work)

## Development Setup

1. Install UV package manager:
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

2. Clone the repository:
```bash
git clone https://github.com/beshu-tech/deltaglider.git
cd deltaglider
```

3. Install development dependencies:
```bash
uv pip install -e ".[dev]"
```

4. Run tests:
```bash
uv run pytest
```

5. Run linting:
```bash
uv run ruff check .
uv run ruff format .
```

6. Run type checking:
```bash
uv run mypy src
```

## Testing

- Write tests for any new functionality
- Ensure all tests pass before submitting PR
- Aim for >90% test coverage for new code
- Use `pytest` for testing

### Running specific test categories:
```bash
# Unit tests only
uv run pytest -m unit

# Integration tests
uv run pytest -m integration

# End-to-end tests (requires Docker)
docker-compose up -d
uv run pytest -m e2e
```

## Code Style

- We use `ruff` for linting and formatting
- Follow PEP 8 guidelines
- Use type hints for all function signatures
- Write docstrings for all public functions and classes

## Documentation

### SDK Documentation

The SDK documentation is located in `docs/sdk/` and includes:
- Getting Started guide
- API Reference
- Examples and use cases
- Architecture overview

When making changes to the Python SDK:
1. Update relevant documentation in `docs/sdk/`
2. Update docstrings in the code
3. Run `make generate` in `docs/sdk/` to update auto-generated docs

## Pull Request Process

1. Update the README.md with details of changes to the interface, if applicable
2. Update the docs/ with any new functionality
3. Update SDK documentation if you've modified the client API
4. The PR will be merged once you have the sign-off of at least one maintainer

## Performance Considerations

DeltaGlider is performance-critical software. When contributing:

- Profile your changes if they affect the core delta engine
- Consider memory usage for large files
- Test with real-world data sizes (GB-scale files)
- Document any performance implications

## Ideas for Contribution

### Good First Issues
- Add support for more file extensions in delta detection
- Improve error messages and user feedback
- Add progress bars for large file operations
- Write more integration tests

### Advanced Features
- Implement parallel delta generation
- Add support for other diff algorithms beyond xdelta3
- Create a web UI for managing deltafied files
- Implement cloud-native reference management
- Add support for other S3-compatible providers (Backblaze B2, Wasabi)

## Questions?

Feel free to open an issue with the "question" label or reach out to the maintainers at support@beshu.tech.

## License

By contributing, you agree that your contributions will be licensed under the MIT License.
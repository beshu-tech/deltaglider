# Publishing DeltaGlider to PyPI

## Prerequisites

1. Create PyPI account at https://pypi.org
2. Create API token at https://pypi.org/manage/account/token/
3. Install build tools:
```bash
pip install build twine
```

## Build the Package

```bash
# Clean previous builds
rm -rf dist/ build/ *.egg-info/

# Build source distribution and wheel
python -m build

# This creates:
# - dist/deltaglider-0.1.0.tar.gz (source distribution)
# - dist/deltaglider-0.1.0-py3-none-any.whl (wheel)
```

## Test with TestPyPI (Optional but Recommended)

1. Upload to TestPyPI:
```bash
python -m twine upload --repository testpypi dist/*
```

2. Test installation:
```bash
pip install --index-url https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple/ deltaglider
```

## Upload to PyPI

```bash
# Upload to PyPI
python -m twine upload dist/*

# You'll be prompted for:
# - username: __token__
# - password: <your-pypi-api-token>
```

## Verify Installation

```bash
# Install from PyPI
pip install deltaglider

# Test it works
deltaglider --help
```

## GitHub Release

After PyPI release, create a GitHub release:

```bash
git tag -a v0.1.0 -m "Release version 0.1.0"
git push origin v0.1.0
```

Then create a release on GitHub:
1. Go to https://github.com/beshu-tech/deltaglider/releases
2. Click "Create a new release"
3. Select the tag v0.1.0
4. Add release notes from CHANGELOG
5. Attach the wheel and source distribution from dist/
6. Publish release

## Version Bumping

For next release:
1. Update version in `pyproject.toml`
2. Update CHANGELOG
3. Commit changes
4. Follow steps above

## Automated Release (GitHub Actions)

Consider adding `.github/workflows/publish.yml`:

```yaml
name: Publish to PyPI

on:
  release:
    types: [published]

jobs:
  publish:
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v4
    - uses: actions/setup-python@v4
      with:
        python-version: '3.11'
    - name: Install dependencies
      run: |
        pip install build twine
    - name: Build package
      run: python -m build
    - name: Publish to PyPI
      env:
        TWINE_USERNAME: __token__
        TWINE_PASSWORD: ${{ secrets.PYPI_API_TOKEN }}
      run: |
        twine upload dist/*
```

## Marketing After Release

1. **Hacker News**: Post with compelling title focusing on the 99.9% compression
2. **Reddit**: r/Python, r/devops, r/aws
3. **Twitter/X**: Tag AWS, Python, and DevOps influencers
4. **Dev.to / Medium**: Write technical article about the architecture
5. **PyPI Description**: Ensure it's compelling and includes the case study link
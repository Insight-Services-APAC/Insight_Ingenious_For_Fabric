```bash
# Clone the repository
git clone <repository-url>
cd ingen_fab

# Install with uv (recommended)
uv sync

# Or install with pip
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
pip install -e .[dev]
```

For complete installation instructions, see the [Installation Guide](../user_guide/installation.md).
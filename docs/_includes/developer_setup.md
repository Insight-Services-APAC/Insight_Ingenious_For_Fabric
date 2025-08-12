=== "macOS/Linux"

    ```bash
    # Clone the repository
    git clone https://github.com/Insight-Services-APAC/Insight_Ingenious_For_Fabric.git
    cd ingen_fab

    # Set up development environment with uv (recommended)
    uv sync --all-extras

    # Or use pip with virtual environment
    python -m venv .venv
    source .venv/bin/activate
    pip install -e .[dev,docs,dbt]

    # Install pre-commit hooks
    pre-commit install

    # Verify installation
    ingen_fab --help
    ```

=== "Windows"

    ```powershell
    # Clone the repository
    git clone https://github.com/Insight-Services-APAC/Insight_Ingenious_For_Fabric.git
    cd ingen_fab

    # Set up development environment with uv (recommended)
    uv sync --all-extras

    # Or use pip with virtual environment
    python -m venv .venv
    .venv\Scripts\activate
    pip install -e .[dev,docs,dbt]

    # Install pre-commit hooks
    pre-commit install

    # Verify installation
    ingen_fab --help
    ```
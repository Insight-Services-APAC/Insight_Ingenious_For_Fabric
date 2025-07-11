# Documentation

This directory contains the MkDocs-based documentation for the Ingenious Fabric Accelerator.

## Development

To work on the documentation:

1. **Install dependencies**:
   ```bash
   uv sync --group docs
   # or
   pip install mkdocs mkdocs-material mkdocs-material-extensions
   ```

2. **Serve locally**:
   ```bash
   mkdocs serve
   # or use the provided script
   ./serve-docs.sh
   ```

3. **Build documentation**:
   ```bash
   mkdocs build
   ```

4. **Deploy to GitHub Pages**:
   ```bash
   mkdocs gh-deploy
   ```

## Structure

- `docs/` - Documentation source files
- `mkdocs.yml` - MkDocs configuration
- `site/` - Generated static site (auto-generated)

## Adding Content

1. Create new `.md` files in the appropriate directory
2. Update `mkdocs.yml` navigation if needed
3. Test locally with `mkdocs serve`
4. Build and deploy

## Themes and Styling

The documentation uses Material for MkDocs with custom styling:

- Theme: `mkdocs-material`
- Custom CSS: `docs/assets/stylesheets/extra.css`
- Colors: Custom "insight" color scheme
- Features: Search, navigation, code highlighting, etc.

## Deployment

Documentation is automatically deployed to GitHub Pages when changes are pushed to the main branch via GitHub Actions (`.github/workflows/docs.yml`).
#!/bin/bash

# Simple script to serve documentation locally
echo "Starting MkDocs development server..."
echo "Documentation will be available at: http://localhost:8000"
echo "Press Ctrl+C to stop the server"

mkdocs serve --dev-addr=0.0.0.0:8000

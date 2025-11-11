#!/usr/bin/env bash
# build.sh

# 1. Install all the Python libraries
pip install -r requirements.txt

# 2. Install Playwright browsers (WITHOUT system dependencies)
playwright install chromium
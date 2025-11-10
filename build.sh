#!/usr/bin/env bash
# build.sh (Updated for Render)

# 1. Install all the Python libraries
pip install -r requirements.txt

# 2. Install Playwright browsers AND their OS dependencies
#    This is the critical fix for Render's environment
playwright install chromium --with-deps
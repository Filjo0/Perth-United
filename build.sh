#!/usr/bin/env bash
# build.sh

# Install all the Python libraries
pip install -r requirements.txt

# Install the Chromium browser driver for Playwright
playwright install chromium
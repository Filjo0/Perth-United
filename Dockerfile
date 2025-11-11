# Dockerfile

# 1. Start from the official Microsoft Playwright image
# This image has Python, Playwright, AND all browser dependencies pre-installed.
FROM mcr.microsoft.com/playwright/python:v1.44.0-jammy

# 2. Set the working directory inside the container
WORKDIR /app

# 3. Copy your requirements file first and install Python libraries
# (This is cached by Docker to speed up future builds)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 4. Copy the rest of your application code into the container
COPY . .

# 5. Tell Render what command to run to start your server
# Render automatically provides the $PORT environment variable
CMD ["python", "app.py"]
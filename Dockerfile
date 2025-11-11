# Dockerfile

# 1. Use the EXACT version Playwright recommended in the error log
FROM mcr.microsoft.com/playwright/python:v1.55.0-jammy

# 2. Set the working directory
WORKDIR /app

# 3. Copy requirements and install Python libraries
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 4. Copy the rest of your application code
COPY . .

# 5. Tell Render what command to run to start your server
# Render automatically provides the $PORT environment variable
CMD ["python", "app.py"]
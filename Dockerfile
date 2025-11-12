# Dockerfile

# 1. Use the NOBLE image (Python 3.12) to support asyncio.TaskGroup
FROM mcr.microsoft.com/playwright/python:v1.55.0-noble

# 2. Set the working directory
WORKDIR /app

# 3. Copy requirements and install Python libraries
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 4. Copy the rest of your application code
COPY . .

# 5. Run the app
CMD ["python", "app.py"]
# Backend Dockerfile
FROM python:3.10-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy app code
COPY . .

# Expose port (Zeabur uses PORT env var, but good to expose 8000)
EXPOSE 8000

# Run command
# 使用 shell form 确保环境变量被扩展
CMD uvicorn main:app --host 0.0.0.0 --port ${PORT:-8000}

FROM python:3.12-slim

# Prevent python from writing pyc files and buffering sdout (to see logs in real-time)
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Install Python Dependencies
RUN pip install --no-cache-dir requests pandas beautifulsoup4 loguru prometheus-client psutil

# Copy the enrire project into the container:
COPY . .

# Isurance Policy
RUN mkdir -p data/raw src/logs

# Run the Robot
CMD ["python", "src/scraper.py"]
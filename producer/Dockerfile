# Dockerfile
FROM python:3.9

WORKDIR /app
# Print progress lines immediately so they show up in `docker logs`.
ENV PYTHONUNBUFFERED=1

# Install required packages
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy your producer script into the container
COPY main.py .

# Set the command to run the producer
CMD ["python", "main.py"]
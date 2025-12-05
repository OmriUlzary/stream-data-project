# Use a slim Python image to keep the size small
FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Install dependencies
# sseclient-py: The library for reading the Wikipedia stream
# kafka-python: The library for talking to Kafka
RUN pip install kafka-python sseclient-py

# Copy the python script into the container
COPY producer.py .

# Run the script
# -u forces "unbuffered" mode so logs appear immediately in Kubernetes
CMD ["python", "-u", "producer.py"]
# Use lightweight Python base image
FROM python:3.9-slim

# Set work directory
WORKDIR /app

# Copy code into container
COPY src/ /app

# Install Flask
RUN pip install flask

# Expose the port Flask runs on
EXPOSE 5000

# Command will be provided by docker-compose using CMD override
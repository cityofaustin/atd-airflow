# Base image
FROM python:3.9-slim-buster

RUN apt-get update 
RUN apt-get install -y git vim magic-wormhole aptitude

# Copy the requirements file to the container
COPY requirements.txt .

COPY .gitconfig /root/.gitconfig

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Set the working directory
WORKDIR /app

# Expose the port the app will run on
EXPOSE 5000

# Start the Flask app
CMD ["python", "webhook.py"]

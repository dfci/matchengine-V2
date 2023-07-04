# Use an official Python 3.8 runtime as a parent image
FROM python:3.8-slim-buster

# Set the working directory in the container to /app
WORKDIR /app

# Add the current directory contents into the container at /app
ADD . /app

# Install any needed packages specified in setup.py
RUN pip install .

# Install bash and bash-completion
RUN apt-get update && apt-get install -y bash bash-completion

# Make port 80 available to the world outside this container
EXPOSE 80

# Define environment variable
ENV NAME MatchEngineV2
ENV SECRETS_JSON /app/secrets.json

RUN pip uninstall bson -y
RUN pip uninstall pymongo -y
RUN pip install pymongo==3.8.0

# Run app.py when the container launches
CMD tail -f /dev/null

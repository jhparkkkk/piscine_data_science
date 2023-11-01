# Use the official PostgreSQL image as the base image
FROM postgres:14-alpine

# Install Python, pip3, and any additional dependencies
RUN apk add --no-cache python3 py3-pip

# Install psycopg2
RUN apk add --no-cache postgresql-dev gcc python3-dev musl-dev
RUN pip3 install psycopg2

# Install unzip utility (if not already installed)
RUN apk add --no-cache unzip

# Set a working directory for your operations
WORKDIR /app

# Copy the .zip folder into the container
COPY subject.zip /app/

# Unzip the .zip folder
RUN unzip subject.zip

# Change permissions for all files in the unzipped folder (recursively)
RUN chmod -R +x /app/*

RUN rm subject.zip
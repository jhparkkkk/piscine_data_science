FROM postgres:14-alpine

# install python3 pip3 psycopg2
RUN apk add --no-cache python3 py3-pip
RUN apk add --no-cache postgresql-dev gcc python3-dev musl-dev
RUN pip3 install psycopg2

# Install unzip utility 
RUN apk add --no-cache unzip

# Set a working directory
WORKDIR /app

# Copy folder into the container
COPY subject.zip /app/

# Unzip folder
RUN unzip subject.zip

# Change permissions
RUN chmod -R +x /app/*

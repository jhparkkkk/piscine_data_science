FROM postgres:14-alpine

# install python3 pip3 psycopg2
RUN apk add --no-cache \
    python3 \
    py3-pip \
    postgresql-dev \
    gcc \
    python3-dev \
    musl-dev \
    unzip

RUN pip3 install psycopg2

# Set a working directory
WORKDIR /app

# Copy folder into the container
COPY subject.zip /app/

# Unzip folder
RUN unzip subject.zip

# add data_2023_feb
COPY *.csv /app/subject/customer/

# Change permissions
RUN chmod -R +x /app/*
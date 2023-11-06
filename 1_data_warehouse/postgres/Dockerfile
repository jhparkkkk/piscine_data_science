FROM postgres:14-alpine

# install python3 pip3 psycopg2
RUN apk add --no-cache \
    python3 \
    py3-pip \
    postgresql-dev \
    gcc \
    python3-dev \
    musl-dev

RUN pip3 install requests
RUN pip3 install sqlalchemy
RUN pip3 install pandas
RUN pip3 install psycopg2

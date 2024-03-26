FROM ubuntu:18.04

# Install dependencies
Run apt -y update

# Install python
RUN apt -y install python3 make gcc build-essential unzip flex bison

# Install pip
RUN apt -y install python3-pip

# upgrade pip
RUN pip3 install --upgrade setuptools pip

# Install python dependencies
RUN pip3 install pymysql psycopg2-binary

# Copy the current directory contents into the container at /app
COPY . /app

# Set the working directory to /app
WORKDIR /app

# make tools executable
RUN make compile-tools

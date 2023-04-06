# Base image from docker hub
FROM apache/spark-py:v3.2.1
USER root

# Upgrade packages
RUN pip install --upgrade pip

RUN apt update
RUN apt-get install sudo -y

RUN sudo apt-get update
RUN sudo apt install default-jre -y
RUN sudo apt-get install openjdk-11-jre -y
RUN sudo apt-get install openjdk-11-jdk -y

# Create directories
RUN sudo mkdir -p /app
RUN sudo mkdir -p /app/conf
RUN sudo mkdir -p /app/input
RUN sudo mkdir -p /app/src
RUN sudo mkdir -p /app/tests
RUN sudo mkdir -p /app/output

# Set Working Directory
WORKDIR /

# Copy files to th eworking directory
COPY /src/utilities.py /app/src
COPY /src/tasks_processes.py /app/src
COPY /src/_run_scripts_.py /app/src
COPY /jobs/task_run.py /app
COPY /conf/requirements.txt /app/conf
COPY /input/challenge_metadata.csv /app/input
COPY /input/challenge_data.parquet.gzip /app/input

WORKDIR /app

# Install dependencies
RUN python3 -m pip install -r conf/requirements.txt

CMD ["python3","task_run.py"]
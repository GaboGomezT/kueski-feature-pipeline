FROM python:3.9.8-buster AS py3
FROM openjdk:8-buster

COPY --from=py3 / /

COPY main.py /src/main.py
COPY requirements.txt requirements.txt
ENV STAGE=prod

ARG PYSPARK_VERSION=3.2.0
RUN pip --no-cache-dir install pyspark==${PYSPARK_VERSION}
RUN pip install -r requirements.txt
RUN apt update
RUN apt install curl -y
RUN apt install unzip -y
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
RUN unzip awscliv2.zip
RUN ./aws/install
WORKDIR /src

CMD ["python", "main.py"]
FROM ubuntu:18.04

COPY requirements.txt /tmp/
COPY ./app /app
WORKDIR "/app"

RUN apt-get update
RUN apt-get -y install python3.8
RUN apt-get -y install python3-pip
RUN python3.8 -m pip install --upgrade pip
RUN python3.8 -m pip install -r /tmp/requirements.txt

EXPOSE 80

CMD /app/script.sh

FROM debian:bullseye
LABEL maintainer="dbrawand@nhs.net"
RUN apt-get update && apt-get -y upgrade
RUN apt-get -y install libffi-dev less make wget vim curl python3-dev python3-pip
RUN mkdir /logs
WORKDIR /app
COPY requirements.txt .
RUN pip3 install -r requirements.txt
COPY . .
EXPOSE 5000
CMD ["gunicorn","app:app"]

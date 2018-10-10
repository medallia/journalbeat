# Build Image for journalbeat
FROM golang:1.11

RUN apt-get update && \
  apt-get install -y libsystemd-dev && \
  apt-get install -y ruby ruby-dev rubygems build-essential rpm && \
  gem install fpm
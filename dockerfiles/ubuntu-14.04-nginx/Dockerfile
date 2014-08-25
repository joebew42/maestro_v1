FROM ubuntu:14.04

MAINTAINER Joe Bew <joebew42@gmail.com>

RUN apt-get update
RUN apt-get install -y nginx
RUN echo "\ndaemon off;" >> /etc/nginx/nginx.conf

CMD ["nginx"]
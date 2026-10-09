FROM docker.io/library/alpine:3.22
RUN apk add --no-cache busybox-extras && mkdir -p /www && printf 'container-ready' > /www/index.html
EXPOSE 8080
ENTRYPOINT ["httpd", "-f", "-p", "8080", "-h", "/www"]

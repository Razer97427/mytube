FROM nginx:alpine

COPY index.html /usr/share/nginx/html/index.html
COPY manifest.json /usr/share/nginx/html/manifest.json
COPY icon-192.png /usr/share/nginx/html/icon-192.png 2>/dev/null || true
COPY nginx.conf /etc/nginx/conf.d/default.conf

EXPOSE 80

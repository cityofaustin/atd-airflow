FROM ubuntu:latest

ENV camera_url="https://cctv.austinmobility.io/image/533.jpg"

RUN apt-get update && apt-get install -y \
    curl \
    imagemagick \
    && rm -rf /var/lib/apt/lists/*

COPY entry_point.sh /entry_point.sh
COPY traffic-light.png /opt/traffic-light.png

CMD /entry_point.sh

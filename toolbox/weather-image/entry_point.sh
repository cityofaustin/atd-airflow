#!/bin/sh

echo "We're downloading images."
curl https://cctv.austinmobility.io/image/533.jpg --output /tmp/raw.jpg -s
echo "We're marking them up.";
convert /tmp/raw.jpg /opt/traffic-light.png -gravity NorthWest -geometry 64x64+10+10 -composite /tmp/icon_on_image.jpg
convert /tmp/icon_on_image.jpg -gravity North -pointsize 40 -annotate +0+10 '38th & Duval' /tmp/icon_and_text.jpg
echo "We're moving them to the right place.";
cp /tmp/icon_and_text.jpg /opt/weather/annotated-image.jpg

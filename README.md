# melcloud-mqtt.py
Python script to translate MQTT command to MELCloud and MELCloud status to MQTT topics

Requires paho-mqtt and the excellent pymelcloud to do the heavy lifting.

It exposes heating and hot water controls and a lot of sensors.

So far, it only supports the first Air-To-Water device in your melcloud account. I am open
to adding support for Air-To-Air and multiple devices, but I don't need it myself and have
no easy way to test it, so won't develop it unless someone is able and willing to test.

Device autodiscovery works with Domoticz and should work with Home Assistant.

Copy the sample.melcloudrc.json to $HOME/.melcloudrc.json and enter the obvious details,
then run the script with python3.  Once it's confirmed working, you'll probably want to add it
to systemd or similar.

If you don't want the script to POST the data to an emoncms, then delete that section
from .melcloudrc.json and it should simply skip that action.

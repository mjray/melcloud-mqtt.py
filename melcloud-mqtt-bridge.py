#!/usr/bin/python
import aiohttp
import asyncio
import pymelcloud
import requests
import json
import random
import paho.mqtt.client as mqtt
from queue import Queue
import time
import math
import sys
import os
from datetime import datetime
# from inspect import getmembers
# from pprint import pprint


def getwobs(dev):
    return (dev.zones[0]._device_state()['WeatherObservations'])


def getfromwobs(wobs, ele, period=0):
    try:
        return (wobs[period][ele])
    except IndexError:
        return (wobs[0][ele])


def set_hot_water_target(dev, temperature):
    return (dev.set({
        pymelcloud.atw_device.PROPERTY_TARGET_TANK_TEMPERATURE:
        temperature
    }))


def set_force_hot_water(dev):
    return (dev.set({
        pymelcloud.atw_device.PROPERTY_OPERATION_MODE:
        pymelcloud.atw_device.OPERATION_MODE_FORCE_HOT_WATER
        # 'ProhibitHotWater':
        # False
    }))


def log_file_and_mqtt(msg, mqttc):
    when = datetime.now()
    print("%s: %s" % (when.isoformat(), msg), file=sys.stderr)
    mqttc.publish("melcloud/status/info",
                  "%s %s" % (when.strftime("%H%M"), msg))


sensor_props = {
    "RoomTemperatureZone1": "C",
    "EcoHotWater": "bool",
    "WaterPump1Status": "bool",
    "WaterPump2Status": "bool",
    "WaterPump3Status": "bool",
    "WaterPump4Status": "bool",
    "SetTankWaterTemperature": "C",
    "TankWaterTemperature": "C",
    "OperationMode": "int",
    "DefrostMode": "int",
    "FlowTemperature": "C",
    "ReturnTemperature": "C",
    "CurrentEnergyConsumed": "Wh/min",
    "CurrentEnergyProduced": "Wh/min",
    "DemandPercentage": "%",
    "HeatPumpFrequency": "%",
    "DailyHeatingEnergyConsumed": "kWh",
    "DailyHeatingEnergyProduced": "kWh",
    "HeatingEnergyConsumedRate1": "Wh/min",
    "HeatingEnergyProducedRate1": "Wh/min",
    "HotWaterEnergyConsumedRate1": "Wh/min",
    "HotWaterEnergyProducedRate1": "Wh/min",
    "HeatingEnergyConsumedRate2": "Wh/min",
    "HeatingEnergyProducedRate2": "Wh/min",
    "HotWaterEnergyConsumedRate2": "Wh/min",
    "HotWaterEnergyProducedRate2": "Wh/min",
    "DailyHotWaterEnergyConsumed": "kWh",
    "DailyHotWaterEnergyProduced": "kWh",
    "TargetHCTemperatureZone1": "C",
    "SetHeatFlowTemperatureZone1": "C",
    "OutdoorTemperature": "C",
    "OutdoorTemperatureForecast1": "C",
    "OutdoorTemperatureForecast2": "C"
}
mqtt_sensor_config_types = {
    "C": "temperature",
    "Wh/min": "energy",
    "kWh": "energy",
    "%": "",
    "bool": "",
    "int": "",
}
mqtt_sensor_config = """
{"device_class": "%s",
 "unique_id": "%s-%s",
 "dev": {
     "ids": "%s",
     "name": "MELCloud MQTT Bridge"
 },
 "name": "%s",
 "state_topic": "melcloud/status/values",
 "unit_of_measurement": "%s",
 "value_template": "{{ value_json.%s}}"
}
"""
actions = ["idle", "heating"]
modes = ["auto", "auto", "heat"]


def mqtt_control_config(uid):
    return({
        "name": "MELCloud MQTT",
        "unique_id": uid,
        "dev": {
            "ids": uid,
            "name": "MELCloud MQTT Bridge"
        },
        "modes": ["auto", "heat"],
        "mode_command_topic": "melcloud/control/mode",
        "mode_state_topic": "melcloud/status/mode",
        "json_attributes_topic": "melcloud/status/values",
        # "json_attributes_template": "{{ value_json }}", is incorrect
        "precision": 0.5,
        "temperature_command_topic": "melcloud/control/temperature",
        "temperature_state_topic": "melcloud/status/values",
        "temperature_state_template":
            "{{ value_json.TargetHCTemperatureZone1 }}",
        "current_temperature_topic": "melcloud/status/values",
        "current_temperature_template":
            "{{ value_json.RoomTemperatureZone1 }}",
        "action_topic": "melcloud/status/action",
    })


def mqtt_water_control_config(uid):
    return({
        "name": "MELCloud MQTT Hot Water",
        "unique_id": uid+"-HW",
        "dev": {
            "ids": uid,
            "name": "MELCloud MQTT Bridge",
        },
        "modes": ["auto", "heat"],
        "mode_command_topic": "melcloud/control/water",
        "mode_state_topic": "melcloud/status/water",
        "json_attributes_topic": "melcloud/status/values",
        # "json_attributes_template": "{{ value_json }}", is incorrect
        "precision": 1.0,
        "temperature_command_topic": "melcloud/control/tank_temperature",
        "temperature_state_topic": "melcloud/status/values",
        "temperature_state_template":
            "{{ value_json.SetTankWaterTemperature }}",
        "current_temperature_topic": "melcloud/status/values",
        "current_temperature_template":
            "{{ value_json.TankWaterTemperature }}",
        "action_topic": "melcloud/status/water_action",
    })


async def main():

    config = json.load(open(os.getenv("HOME", ".")+"/.melcloudrc.json"))

    q = Queue()

    def on_message(client, userdata, message):
        q.put("%s: %s" % (message.topic, str(message.payload.decode("utf-8"))))

    mqttc = mqtt.Client("melcloud-mqtt")
    mqttc.username_pw_set(config["mqtt"]["username"],
                          config["mqtt"]["password"])
    mqttc.connect(config["mqtt"]["host"], port=config["mqtt"]["port"])
    mqttc.subscribe("melcloud/control/#")
    mqttc.on_message = on_message
    mqttc.loop_start()
    if (config["autodiscovery"]["enabled"]):
        mqttc.publish(
            "homeassistant/climate/melcloud/config",
            json.dumps(mqtt_control_config(
                            config["autodiscovery"]["uid"]),
                       separators=(",", ":")),
            retain=True)
        mqttc.publish(
            "homeassistant/climate/melcloud_water/config",
            json.dumps(mqtt_water_control_config(
                            config["autodiscovery"]["uid"]),
                       separators=(",", ":")),
            retain=True)
        if (config["autodiscovery"]["all"]):
            for key, type in sensor_props.items():
                mqttc.publish(
                    "homeassistant/sensor/%s/config" % key.lower(),
                    mqtt_sensor_config % (
                        mqtt_sensor_config_types[type],
                        config["autodiscovery"]["uid"],
                        key.lower(),
                        config["autodiscovery"]["uid"],
                        key,
                        type,
                        key),
                    retain=True)

    buttons = []
    set_temp = None
    async with aiohttp.ClientSession() as session:
        # call the login method with the session
        token = await pymelcloud.login(
                config["melcloud"]["username"],
                config["melcloud"]["password"],
                session=session)
        # lookup the device
        devices = await pymelcloud.get_devices(token, session=session)
        device = devices[pymelcloud.DEVICE_TYPE_ATW][0]
        update_seconds = random.randint(0, 59)
        log_file_and_mqtt("MELCloud Bridge connected and running", mqttc)
        sys.stderr.flush()
        while True:
            if (math.floor(time.time()) % 120) == update_seconds:
                # perform logic on the device
                await device.update()
                thistemp = device.outside_temperature
                wobs = getwobs(device)
                nexttemp = getfromwobs(wobs, "Temperature")
                futuretemp = getfromwobs(wobs, "Temperature", 1)
                roomzone = device.zones[0]
                roomtemp = roomzone.room_temperature
                nextmode = roomzone.operation_mode
                # Handle any control messages that have arrived
                while not q.empty():
                    message = q.get()
                    if message is None:
                        continue
                    elif ((message == "melcloud/control/pause: off")
                          and ("P" in buttons)):
                        buttons.remove("P")
                    elif ((message == "melcloud/control/pause: on")
                          and ("P" not in buttons)):
                        buttons.add("P")
                    elif (message == "melcloud/control/mode: heat"):
                        nextmode = "curve"
                    elif (message.startswith("melcloud/control/mode: ")):
                        nextmode = "heat-thermostat"
                    elif (message.startswith("melcloud/control/temperature:")):
                        set_temp = float(message.split(" ")[-1])
                    elif (message.startswith(
                            "melcloud/control/tank_temperature")):
                        await set_hot_water_target(device,
                                                   message.split(" ")[-1])
                    elif (message == "melcloud/control/water: heat"):
                        await set_force_hot_water(device)
                    log_file_and_mqtt("MQTT %s" % (message.replace(":", "")),
                                      mqttc)
                # consider heating temperature and mode
                if (not device.holiday_mode
                        and not (device.status == "defrost")):
                    # Switch main heating between thermostat, curve and flow
                    # modes based on MQTT commands. Set thermostat
                    # temperature low, like 16 degrees, as a safeguard for
                    # a dead process or melcloud connection, and we can use
                    # heat-thermostat mode here as a proxy for "off".
                    # Temperatures over 26 are regarded as flow targets and
                    # 26 and below as room targets.
                    if set_temp and set_temp > 26:
                        nextmode = "heat-flow"
                        await roomzone.set_target_heat_flow_temperature(
                            set_temp)
                        set_temp = None
                    elif set_temp and set_temp <= 26:
                        nextmode = "heat-thermostat"
                        await roomzone.set_target_temperature(set_temp)
                        set_temp = None
                    if nextmode != roomzone.operation_mode:
                        await roomzone.set_operation_mode(nextmode)
                        log_file_and_mqtt(
                            "%s at %s to %s" % (nextmode, roomtemp, nexttemp),
                            mqttc)
                    sys.stderr.flush()
                # pprint(device._device_conf)
                data = {
                    "OutdoorTemperature": thistemp,
                    "OutdoorTemperatureForecast1": nexttemp,
                    "OutdoorTemperatureForecast2": futuretemp,
                }
                interesting = device._device_conf["Device"]
                for key in sensor_props:
                    if key not in data:
                        try:
                            v = float(interesting[key])
                            data[key] = v
                        except:
                            try:
                                b = bool(interesting[key])
                                v = 0
                                if b:
                                    v = 1
                                data[key] = v
                            except:
                                log_file_and_mqtt(
                                    key + " is not numeric or boolean", mqttc)
                    # data[key] = device._device_conf["Device"][key]
                # print(json.dumps(data), file=sys.stderr)
                datastr = json.dumps(data, separators=(",", ":"))
                if "emoncms" in config.keys():
                    reply = requests.get(
                        config["emoncms"]["posturl"], {
                            "apikey": config["emoncms"]["apikey"],
                            "node": config["emoncms"]["node"],
                            "data": datastr
                        },
                        timeout=60)
                    if reply.text != "ok":
                        log_file_and_mqtt("%s sending %s" %
                                          (reply.text, datastr),
                                          mqttc)
                        sys.stderr.flush()
                mqttc.publish(
                    "melcloud/status/mode", "heat" if
                    ((nextmode != "heat-thermostat")
                     or (int(data["OperationMode"]) == 2)) else "auto")
                mqttc.publish(
                    "melcloud/status/water", "heat" if
                    (int(data["OperationMode"]) == 1) else "auto")
                mqttc.publish("melcloud/status/values", datastr)
                mqttc.publish("melcloud/status/action",
                              actions[int(data["WaterPump1Status"])])
                time.sleep(1.1)
            time.sleep(0.1)
        await session.close()
        mqttc.loop_stop()
print(
    "%s: Startup logic 20221219 in pid %s" % (datetime.now().isoformat(),
                                              os.getpid()),
    file=sys.stderr)
loop = asyncio.get_event_loop()
loop.run_until_complete(main())

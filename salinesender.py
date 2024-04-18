import paho.mqtt.client as mqtt
import json

# MQTT broker details
host = "f3eaff72b74e4a458c40a4965c3636c2.s2.eu.hivemq.cloud"
port = 8883
username = "test_"
password = "Test@123"
topic = "/updateVariable"

# Data to be sent
data = {
    "deviceid": 10,
    "DPM": 1,
    "BATTERY": 100,
    "CONSUMEDCAPACITY": 55,
    "REMAININGCAPACITY": 56,
    "TEMPERATURE": 32,
    "HUMIDITY": 43
}

def on_publish(client, userdata, mid):
    print("Data published with message ID:", mid)

client = mqtt.Client()
client.username_pw_set(username, password)
client.on_publish = on_publish

client.tls_set()
client.connect(host, port)

client.loop_start()  # Start the loop

payload = json.dumps(data)
result = client.publish(topic, payload, qos=1)  # Specify QoS level

if result.rc == mqtt.MQTT_ERR_SUCCESS:
    print("Data published successfully.")
else:
    print("Failed to publish data. Error:", result.rc)

client.loop_stop()  # Stop the loop
client.disconnect()

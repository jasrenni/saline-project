import paho.mqtt.client as mqtt
import json
import psycopg2

# MQTT broker details
mqtt_host = "f3eaff72b74e4a458c40a4965c3636c2.s2.eu.hivemq.cloud"
mqtt_port = 8883
mqtt_username = "test_"
mqtt_password = "Test@123"
mqtt_topic = "/updateVariable"

# PostgreSQL database details
db_host = "dpg-civsou15rnuqala0jlb0-a.oregon-postgres.render.com"
db_port = 5432
db_name = "saline"
db_username = "admin"
db_password = "Exuvz9v0mhFF1mnQsEtMQ4OmQ0ehEtaP"

def on_connect(client, userdata, flags, rc):
    print("Connected to MQTT Broker with result code " + str(rc))
    client.subscribe(mqtt_topic)

def on_message(client, userdata, msg):
    try:
        payload = msg.payload.decode("utf-8")
        data = json.loads(payload)

        # Extract data fields from MQTT message
        device_id = data.get("deviceid")
        dpm = data.get("DPM")
        battery = data.get("BATTERY")
        consumed_capacity = data.get("CONSUMEDCAPACITY")
        remaining_capacity = data.get("REMAININGCAPACITY")
        temperature = data.get("TEMPERATURE")
        humidity = data.get("HUMIDITY")

        # Establish a connection to the PostgreSQL database
        conn = psycopg2.connect(
            host=db_host,
            port=db_port,
            database=db_name,
            user=db_username,
            password=db_password
        )
        cursor = conn.cursor()

        # Update the Variable table
        update_query = """
            UPDATE variable
            SET "DPM" = %s,
                "BATTERY" = %s,
                "CONSUMEDCAPACITY" = %s,
                "REMAININGCAPACITY" = %s,
                "TEMPERATURE" = %s,
                "HUMIDITY" = %s
            WHERE "DEVICEID" = %s;
        """
        cursor.execute(update_query, (dpm, battery, consumed_capacity, remaining_capacity, temperature, humidity, device_id))
        conn.commit()

        print("Data updated in Variable table")

        cursor.close()
        conn.close()

    except Exception as e:
        print("Error processing MQTT message:", e)

client = mqtt.Client()
client.username_pw_set(mqtt_username, mqtt_password)
client.on_connect = on_connect
client.on_message = on_message

client.tls_set()
client.connect(mqtt_host, mqtt_port)

client.loop_forever()

import os
import json
from time import sleep
from datetime import datetime
from typing import Any, Dict
import jsonschema

import couchdb
import schedule
import paho.mqtt.client as mqtt

from base_mqtt_pub_sub import BaseMQTTPubSub


class CouchDBSaverPubSub(BaseMQTTPubSub):
    def __init__(
        self: Any,
        sensor_save_topic: str,
        telemetry_save_topic: str,
        audio_save_topic: str,
        couchdb_error_topic: str,
        couchdb_user: str,
        couchdb_password: str,
        couchdb_server_ip: str,
        device_ip: str,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)

        self.sensor_save_topic = sensor_save_topic
        self.telemetry_save_topic = telemetry_save_topic
        self.audio_save_topic = audio_save_topic
        self.couchdb_error_topic = couchdb_error_topic
        self.couchdb_user = couchdb_user
        self.couchdb_password = couchdb_password
        self.couchdb_server_ip = couchdb_server_ip
        self.device_ip = device_ip

        with open("couchdb_saver.schema", "r", encoding="utf-8") as file_pointer:
            self.schema = json.loads(file_pointer.read())

        self.connect_client()
        sleep(1)
        self.publish_registration("CouchDB Saver Registration")

    def _to_save_callback(
        self: Any, _client: mqtt.Client, _userdata: Dict[Any, Any], msg: Any
    ) -> None:

        payload_json_str = json.loads(str(msg.payload.decode("utf-8")))
        try:
            jsonschema.validate(instance=payload_json_str, schema=self.schema)
        except jsonschema.exceptions.ValidationError as err:
            self.publish_to_topic(self.couchdb_error_topic, err)

        couch = couchdb.Server(f"http://admin:PASSWORD@{self.device_ip}:5984/")
        database = (
            couch.create("aisonobuoy")
            if "aisonobuoy" not in couch
            else couch["aisonobuoy"]
        )
        database.save(payload_json_str)

    def main(self: Any) -> None:
        schedule.every(10).seconds.do(
            self.publish_heartbeat, payload="CouchDB Saver Heartbeat"
        )

        self.add_subscribe_topics(
            [self.sensor_save_topic, self.telemetry_save_topic, self.audio_save_topic],
            [self._to_save_callback, self._to_save_callback, self._to_save_callback],
            [2, 2, 2],
        )

        while True:
            try:
                schedule.run_pending()
                sleep(0.001)
            except Exception as e:
                print(e)


if __name__ == "__main__":
    saver = CouchDBSaverPubSub(
        sensor_save_topic=os.environ.get("SENSOR_SAVE_TOPIC"),
        telemetry_save_topic=os.environ.get("TELEMETRY_SAVE_TOPIC"),
        audio_save_topic=os.environ.get("AUDIO_SAVE_TOPIC"),
        couchdb_error_topic=os.environ.get("COUCHDB_ERROR_TOPIC"),
        couchdb_user=os.environ.get("COUCHDB_USER"),
        couchdb_password=os.environ.get("COUCHDB_PASSWORD"),
        couchdb_server_ip=os.environ.get("COUCHDB_SERVER_IP_ADDR"),
        device_ip=os.environ.get("DEVICE_IP"),
        mqtt_ip=os.environ.get("MQTT_IP"),
    )
    saver.main()

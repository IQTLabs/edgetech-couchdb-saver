import os
import json
from time import sleep
from datetime import datetime
from typing import Any, Dict

import couchdb
import schedule
import paho.mqtt.client as mqtt

from base_mqtt_pub_sub import BaseMQTTPubSub


class CouchDBSaverPubSub(BaseMQTTPubSub):
    def __init__(self: Any, to_save_topic: str, device_ip: str, **kwargs: Any) -> None:
        super().__init__(**kwargs)

        self.to_save_topic = to_save_topic
        self.device_ip = device_ip

        self.connect_client()
        sleep(1)
        self.publish_registration("CouchDB Saver Registration")

    def _to_save_callback(
        self: Any, _client: mqtt.Client, _userdata: Dict[Any, Any], msg: Any
    ) -> None:
        payload_json_str = json.loads(str(msg.payload.decode("utf-8")))
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

        self.add_subscribe_topic(self.to_save_topic, self._to_save_callback)

        # self.add_subscribe_topics(
        #    [self.to_save_topic, self.c2c_topic],
        #    [self._to_save_callback, self._c2c_callback],
        #    [2, 2],
        # )

        while True:
            try:
                schedule.run_pending()
                sleep(0.001)
            except Exception as e:
                print(e)


if __name__ == "__main__":
    saver = CouchDBSaverPubSub(
        to_save_topic=os.environ.get("TO_SAVE_TOPIC"),
        device_ip=os.environ.get("DEVICE_IP"),
        mqtt_ip=os.environ.get("MQTT_IP"),
    )
    saver.main()

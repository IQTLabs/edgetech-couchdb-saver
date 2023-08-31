"""This file contains the CouchDBSaverPubSub class which is a child class of BaseMQTTPubSub. 
The CouchDBSaverPubSub creates an MQTT client that subscribes to data topics and saves them
to a local running CouchDB sever.
"""
import os
import json
from time import sleep
from typing import Any, Dict

import jsonschema
import couchdb
import schedule
import paho.mqtt.client as mqtt
import logging

from base_mqtt_pub_sub import BaseMQTTPubSub


class CouchDBSaverPubSub(BaseMQTTPubSub):
    """This class creates a connection to the MQTT broker and subscribes to several topics and
    saves their messages to the local CouchDB database.

    Args:
        BaseMQTTPubSub (BaseMQTTPubSub): parent class written in the EdgeTech Core module
    """

    def __init__(
        self: Any,
        sensor_save_topic: str,
        telemetry_save_topic: str,
        audio_base64_topic: str,
        image_base64_topic: str,
        couchdb_error_topic: str,
        couchdb_user: str,
        couchdb_password: str,
        couchdb_server_ip: str,
        couchdb_database_name: str,
        device_ip: str,
        debug: bool = False,
        **kwargs: Any,
    ) -> None:
        """The CouchDBSaverPubSub takes MQTT topics to write data from and CouchDB authentication
        information to write data sent on topics to the local CouchDB database.

        Args:
            sensor_save_topic (str): the MQTT sensor topic to subscribe to and write the
            data sent to the database
            telemetry_save_topic (str): the MQTT telemetry topic to subscribe to and
            write the data sent to the database
            audio_base64_topic (str): the MQTT audio file topic to subscribe to and write
            the data sent to the database
            couchdb_error_topic (str): the MQTT topic to broadcast any CouchDB errors onto
            couchdb_user (str): the username for CouchDB authentication
            couchdb_password (str): the password for CouchDB authentication
            couchdb_server_ip (str): the IP address for couchDB authentication
            device_ip (str): the IP of the current device
        """
        # to override any of the BaseMQTTPubSub attributes
        super().__init__(**kwargs)

        # assign constructor parameters to class attributes
        self.sensor_save_topic = sensor_save_topic
        self.telemetry_save_topic = telemetry_save_topic
        self.audio_base64_topic = audio_base64_topic
        self.image_base64_topic = image_base64_topic
        self.couchdb_error_topic = couchdb_error_topic
        self.couchdb_user = couchdb_user
        self.couchdb_password = couchdb_password
        self.couchdb_server_ip = couchdb_server_ip
        self.couchdb_database_name = couchdb_database_name
        self.device_ip = device_ip
        self.debug = debug

        # open schema file for validation
        with open("couchdb_saver.schema", "r", encoding="utf-8") as file_pointer:
            self.schema = json.loads(file_pointer.read())

        # setup MQTT client connection
        self.connect_client()
        sleep(1)
        self.publish_registration("CouchDB Saver Registration")

    def _to_save_callback(
        self: Any, _client: mqtt.Client, _userdata: Dict[Any, Any], msg: Any
    ) -> None:
        """Callback to write data sent on a topic to the local CouchDB database.

        Args:
            _client (mqtt.Client): the MQTT client that was instantiated in the constructor.
            _userdata (Dict[Any,Any]): data passed to the callback through the MQTT paho Client
            class constructor or set later through user_data_set().
            msg (Any): the received message over the subscribed channel that includes
            the topic name and payload after decoding. The messages here will include the
            sensor data to save.
        """
        # decode payload string
        payload_json_str = json.loads(str(msg.payload.decode("utf-8")))
        try:
            # validate payload JSON against JSON schema
            jsonschema.validate(instance=payload_json_str, schema=self.schema)
        except jsonschema.exceptions.ValidationError as err:
            logging.warn(f"JSON validation failed for {payload_json_str}")
            # if self.debug:
            # send validation errors on CouchDB error topic
            self.publish_to_topic(self.couchdb_error_topic, str(err))

        # connect to local CouchDB instance
        couch = couchdb.Server(
            f"http://{self.couchdb_user}:{self.couchdb_password}@{self.device_ip}:5984/"
        )
        database = (
            couch.create(self.couchdb_database_name)
            if self.couchdb_database_name not in couch
            else couch[self.couchdb_database_name]
        )
        # write to DB
        _id, _ = database.save(payload_json_str)
        logging.info(f"Document inserted at {_id}")

    def main(self: Any) -> None:
        """Main loop and function that setup the heartbeat to keep the TCP/IP
        connection alive and publishes the data to the MQTT broker and keeps the
        main thread alive."""
        # setup heartbeat to keep TCP/IP connection alive
        schedule.every(10).seconds.do(
            self.publish_heartbeat, payload="CouchDB Saver Heartbeat"
        )

        # subscribe to topics for database writing — callbacks are all the same
        self.add_subscribe_topics(
            [
                self.sensor_save_topic,
                self.telemetry_save_topic,
                self.audio_base64_topic,
                self.image_base64_topic,
            ],
            [
                self._to_save_callback,
                self._to_save_callback,
                self._to_save_callback,
                self._to_save_callback,
            ],
            [2, 2, 2, 2],
        )

        while True:
            try:
                # flush any pending scheduled tasks
                schedule.run_pending()
                # sleep so the loop does not run at CPU time
                sleep(0.001)
            except KeyboardInterrupt as exception:
                if self.debug:
                    print(exception)


if __name__ == "__main__":
    saver = CouchDBSaverPubSub(
        sensor_save_topic=str(os.environ.get("SENSOR_TOPIC")),
        telemetry_save_topic=str(os.environ.get("TELEMETRY_TOPIC")),
        audio_base64_topic=str(os.environ.get("AUDIO_TOPIC")),
        image_base64_topic=str(os.environ.get("IMAGE_TOPIC")),
        couchdb_error_topic=str(os.environ.get("COUCHDB_ERROR_TOPIC")),
        couchdb_user=str(os.environ.get("COUCHDB_USER")),
        couchdb_password=str(os.environ.get("COUCHDB_PASSWORD")),
        couchdb_server_ip=str(os.environ.get("COUCHDB_SERVER_IP_ADDR")),
        couchdb_database_name=str(os.environ.get("COUCHDB_DATABASE_NAME")),
        device_ip=str(os.environ.get("DEVICE_IP")),
        mqtt_ip=str(os.environ.get("MQTT_IP")),
    )
    saver.main()

import argparse
from concurrent.futures import Future
import sys
import threading
import traceback
from time import sleep
from uuid import uuid4
import json

from awscrt import auth, io, mqtt, http
from awsiot import mqtt_connection_builder


# parse command line arguments
parser = argparse.ArgumentParser(description="NOTE: NOT ALL FUNCTIONALITIES ARE ALREADY IMPLEMENTED\n"
                                             "IoT device simulator is built to simulate the functioning of a real IoT "
                                             "device. "
                                             "If it doesn't have a certificate then it will automatically "
                                             "get it using the aws fleet provisioning (creating a new thing during the "
                                             "process). "
                                             "When connected to aws it will simulate various device functions: "
                                             "1) Keep its shadow in sync with aws shadow service.\n"
                                             "2) Subscribe to an MQTT topic\n"
                                             "3) Write to an MQTT topic")

parser.add_argument('--endpoint', required=True, help="Your AWS IoT custom endpoint, not including a port. "
                                                      "Ex: \"w6zbse3vjd5b4p-ats.iot.us-west-2.amazonaws.com\"")

parser.add_argument('--cert',  required=True, help="File path to your client certificate, in PEM format.")

parser.add_argument('--key', required=True, help="File path to your private key file, in PEM format.")

parser.add_argument('--root-ca', required=True, help="File path to root certificate authority, in PEM format.")

parser.add_argument('--thing-name', required=True, help="The name assigned to your IoT Thing")


# callback when connection is accidentally lost
def on_connection_interrupted(connection, error, **kwargs):
    print("Connection interrupted. error: {}".format(error))


# callback when an interrupted connection is re-established.
def on_connection_resumed(connection, return_code, session_present, **kwargs):
    print("Connection resumed. return_code: {} session_present: {}".format(return_code, session_present))

    if return_code == mqtt.ConnectReturnCode.ACCEPTED and not session_present:
        print("Session did not persist. Resubscribing to existing topics...")
        resubscribe_future, _ = connection.resubscribe_existing_topics()

        # Cannot synchronously wait for resubscribe result because we're on the connection's event-loop thread,
        # evaluate result with a callback instead.
        resubscribe_future.add_done_callback(on_resubscribe_complete)


def on_resubscribe_complete(resubscribe_future):
    resubscribe_results = resubscribe_future.result()
    print("Resubscribe results: {}".format(resubscribe_results))

    for topic, qos in resubscribe_results['topics']:
        if qos is None:
            sys.exit("Server rejected resubscribe to topic: {}".format(topic))


# callback when the subscribed topic receives a message
def on_message_received(topic, payload, **kwargs):
    print("Received message from topic '{}': {}".format(topic, payload))


def publish_device_logs(thing_name, endpoint=None, cert=None, key=None, root_ca=None, mqtt_session=None, is_done_event=threading.Event()):
    """Publish simulated device logs to thing's log topic

    :param endpoint: Your AWS IoT custom endpoint, not including a port.
    :param cert: File path to your client certificate, in PEM format.
    :param key: File path to your private key file, in PEM format.
    :param root_ca: File path to root certificate authority, in PEM format.
    :param thing_name: The name assigned to your IoT Thing.
    :param mqtt_session: Existing mqtt session.
    :param is_done_event: Threading Event to stop execution.
    :return: None
    """

    is_done = is_done_event
    mqtt_connection_is_external = False

    # thing's log topic
    log_topic = 'iot/things/' + thing_name + '/logs'

    if mqtt_session is None:
        # spin up resources
        event_loop_group = io.EventLoopGroup(1)
        host_resolver = io.DefaultHostResolver(event_loop_group)
        client_bootstrap = io.ClientBootstrap(event_loop_group, host_resolver)

        client_id = str(uuid4()),

        # set mqtt connection parameters
        mqtt_connection = mqtt_connection_builder.mtls_from_path(
            endpoint=endpoint,
            cert_filepath=cert,
            pri_key_filepath=key,
            client_bootstrap=client_bootstrap,
            ca_filepath=root_ca,
            on_connection_interrupted=on_connection_interrupted,
            on_connection_resumed=on_connection_resumed,
            client_id=client_id,
            clean_session=False,
            keep_alive_secs=6
        )

        print("Connecting to {} with client ID '{}'...".format(endpoint, client_id))

        connect_future = mqtt_connection.connect()

        # future.result() waits until a result is available
        connect_future.result()
        print("Connected!")

    else:
        mqtt_connection = mqtt_session
        mqtt_connection_is_external = True

    # subscribe to the thing's logs topic
    print("Subscribing to topic '{}'...".format(log_topic))
    subscribe_future, packet_id = mqtt_connection.subscribe(
        topic=log_topic,
        qos=mqtt.QoS.AT_LEAST_ONCE,
        callback=on_message_received
    )

    subscribe_result = subscribe_future.result()
    print("Subscribed with {}".format(str(subscribe_result['qos'])))

    # simulate logs publishing to the thing's topic log every pub_delay secs
    pub_delay = 15
    print("Sending logs every {} seconds".format(pub_delay))

    with open('log.json', 'r') as f:
        log_data = json.load(f)
        pub_count = 1

        while True:
            # check quit condition
            if is_done.is_set():
                break

            # publish simulated log
            log_data["test_attribute"] = str(uuid4())
            log_data["publish_count"] = pub_count
            message = json.dumps(log_data)

            print("Publishing message seq: {} to topic: {}".format(pub_count, log_topic))
            mqtt_connection.publish(
                topic=log_topic,
                payload=message,
                qos=mqtt.QoS.AT_LEAST_ONCE
            )

            sleep(pub_delay)
            pub_count += 1

    # if mqtt connection was created here then disconnect it
    if not mqtt_connection_is_external:
        print("Disconnecting...")
        disconnect_future = mqtt_connection.disconnect()
        disconnect_future.result()
        print("Disconnected!")


if __name__ == '__main__':
    args = parser.parse_args()
    publish_device_logs(args.thing_name, args.endpoint, args.cert, args.key, args.root_ca)

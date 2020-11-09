import argparse
from awscrt import auth, io, mqtt, http
from awsiot import iotshadow
from awsiot import mqtt_connection_builder
from concurrent.futures import Future
import sys
import threading
import traceback
from uuid import uuid4
import json


parser = argparse.ArgumentParser(description="Keeps Device Shadow in sync across client and server and allows the user"
                                             "to change the value of a test property")
parser.add_argument('--endpoint', required=True, help="Your AWS IoT custom endpoint, not including a port. " +
                                                      "Ex: \"w6zbse3vjd5b4p-ats.iot.us-west-2.amazonaws.com\"")
parser.add_argument('--cert',  help="File path to your client certificate, in PEM format")
parser.add_argument('--key', help="File path to your private key file, in PEM format")
parser.add_argument('--root-ca', help="File path to root certificate authority, in PEM format. " +
                                      "Necessary if MQTT server uses a certificate that's not already in " +
                                      "your trust store")
parser.add_argument('--thing-name', required=True, help="The name assigned to your IoT Thing")


# Using globals to simplify code
mqtt_connection = None
mqtt_connection_is_external = False
shadow_client = None
thing_name = None
is_done = None

with open('shadow_defaults.json', 'r') as f:
    SHADOW_VALUE_DEFAULT = json.load(f)


class LockedData:
    def __init__(self):
        self.lock = threading.Lock()
        self.shadow_value = None


locked_data = LockedData()


# Function for gracefully quitting the program
def exit(msg_or_exception):
    if isinstance(msg_or_exception, Exception):
        print("Exiting due to exception.")
        traceback.print_exception(msg_or_exception.__class__, msg_or_exception, sys.exc_info()[2])
    else:
        print("Exiting:", msg_or_exception)

    if not mqtt_connection_is_external:
        print("Disconnecting...")
        future = mqtt_connection.disconnect()
        future.add_done_callback(on_disconnected)
    else:
        is_done.set()


def on_disconnected():
    print("Disconnected.")

    # Signal that procedure is finished
    is_done.set()


def on_get_shadow_accepted(response):
    # type: (iotshadow.GetShadowResponse) -> None
    try:
        print("Finished getting initial shadow state.")

        if response.state:

            if response.state.reported:
                print("  Shadow contains reported value '{}'.".format(response.state.reported))
                set_local_value_due_to_initial_query(response.state.reported)

            if response.state.delta:
                print("  Shadow contains delta value '{}'.".format(response.state.delta))
                change_shadow_value(response.state.delta)

            return

        print("  Shadow document lacks properties or does not exist. Setting defaults...")
        change_shadow_value(SHADOW_VALUE_DEFAULT)
        return

    except Exception as e:
        exit(e)


def on_get_shadow_rejected(error):
    # type: (iotshadow.ErrorResponse) -> None
    if error.code == 404:
        print("Thing has no shadow document. Creating with defaults...")
        change_shadow_value(SHADOW_VALUE_DEFAULT)
    else:
        exit("Get request was rejected. code:{} message:'{}'".format(
            error.code, error.message))


def on_shadow_delta_updated(delta):
    # type: (iotshadow.ShadowDeltaUpdatedEvent) -> None
    try:
        print("Received shadow delta event.")
        change_shadow_value(delta.state)

    except Exception as e:
        exit(e)


def on_publish_update_shadow(future):
    # type: (Future) -> None
    try:
        future.result()
        print("Update request published.")
    except Exception as e:
        print("Failed to publish update request.")
        exit(e)


def on_update_shadow_accepted(response):
    # type: (iotshadow.UpdateShadowResponse) -> None
    try:
        print("Finished updating reported shadow value.")  # type: ignore
        print("Enter desired value: ")  # remind user they can input new values
    except:
        exit("Updated shadow is missing the target property.")


def on_update_shadow_rejected(error):
    # type: (iotshadow.ErrorResponse) -> None
    exit("Update request was rejected. code:{} message:'{}'".format(
        error.code, error.message))


def set_local_value_due_to_initial_query(reported_value):
    with locked_data.lock:
        locked_data.shadow_value = reported_value
    print("Enter desired value: ")  # remind user they can input new values


def change_shadow_value(delta_state):
    with locked_data.lock:

        if locked_data.shadow_value is None:
            locked_data.shadow_value = delta_state

        else:
            changed = False

            # check for changes
            try:
                for key in delta_state:
                    if delta_state[key] != locked_data.shadow_value[key]:
                        changed = True
                        break
            except KeyError:
                changed = True

            if not changed:
                print("Local shadow is already updated.")
                print("Enter desired value: ")  # remind user they can input new values
                return

            # update local shadow value
            for key in delta_state:
                locked_data.shadow_value[key] = delta_state[key]
            print("Changed local shadow value.")

        # make a copy to free the locked resource
        shadow_value = locked_data.shadow_value.copy()

    print("Updating reported shadow value...")

    request = iotshadow.UpdateShadowRequest(
        thing_name=thing_name,
        state=iotshadow.ShadowState(
            reported=shadow_value,
            desired=shadow_value,
        )
    )

    future = shadow_client.publish_update_shadow(request, mqtt.QoS.AT_LEAST_ONCE)
    future.add_done_callback(on_publish_update_shadow)


def user_input_thread_fn():
    while True:
        try:
            # Read user input
            new_value = input()

            # If user wants to quit, then quit.
            # Otherwise change the shadow value.
            if new_value in ['exit', 'quit']:
                exit("User has quit")
                break
            else:
                change_shadow_value({'test_property': new_value})

        except Exception as e:
            print("Exception on input thread.")
            exit(e)
            break


def shadow_handler(endpoint, cert, key, root_ca, thing_name_param, mqtt_session=None, is_done_event=threading.Event()):
    # Using globals to simplify code
    global mqtt_connection, mqtt_connection_is_external, shadow_client, thing_name, is_done
    thing_name = thing_name_param
    is_done = is_done_event

    if mqtt_session is None:
        # Spin up resources
        event_loop_group = io.EventLoopGroup(1)
        host_resolver = io.DefaultHostResolver(event_loop_group)
        client_bootstrap = io.ClientBootstrap(event_loop_group, host_resolver)

        client_id = str(uuid4())

        mqtt_connection = mqtt_connection_builder.mtls_from_path(
            endpoint=endpoint,
            cert_filepath=cert,
            pri_key_filepath=key,
            client_bootstrap=client_bootstrap,
            ca_filepath=root_ca,
            client_id=client_id,
            clean_session=False,
            keep_alive_secs=6)

        print("Connecting to {} with client ID '{}'...".format(endpoint, client_id))

        connected_future = mqtt_connection.connect()

        shadow_client = iotshadow.IotShadowClient(mqtt_connection)

        # Wait for connection to be fully established.
        # Note that it's not necessary to wait, commands issued to the
        # mqtt_connection before its fully connected will simply be queued.
        # But this function waits here so it's obvious when a connection
        # fails or succeeds.
        connected_future.result()
        print("Connected!")

    else:
        mqtt_connection = mqtt_session
        mqtt_connection_is_external = True
        shadow_client = iotshadow.IotShadowClient(mqtt_connection)

    try:
        # Subscribe to necessary topics.
        # Note that is **is** important to wait for "accepted/rejected" subscriptions
        # to succeed before publishing the corresponding "request".
        print("Subscribing to Delta events...")
        delta_subscribed_future, _ = shadow_client.subscribe_to_shadow_delta_updated_events(
            request=iotshadow.ShadowDeltaUpdatedSubscriptionRequest(thing_name=thing_name),
            qos=mqtt.QoS.AT_LEAST_ONCE,
            callback=on_shadow_delta_updated)

        # Wait for subscription to succeed
        delta_subscribed_future.result()

        print("Subscribing to Update responses...")
        update_accepted_subscribed_future, _ = shadow_client.subscribe_to_update_shadow_accepted(
            request=iotshadow.UpdateShadowSubscriptionRequest(thing_name=thing_name),
            qos=mqtt.QoS.AT_LEAST_ONCE,
            callback=on_update_shadow_accepted)

        update_rejected_subscribed_future, _ = shadow_client.subscribe_to_update_shadow_rejected(
            request=iotshadow.UpdateShadowSubscriptionRequest(thing_name=thing_name),
            qos=mqtt.QoS.AT_LEAST_ONCE,
            callback=on_update_shadow_rejected)

        # Wait for subscriptions to succeed
        update_accepted_subscribed_future.result()
        update_rejected_subscribed_future.result()

        print("Subscribing to Get responses...")
        get_accepted_subscribed_future, _ = shadow_client.subscribe_to_get_shadow_accepted(
            request=iotshadow.GetShadowSubscriptionRequest(thing_name=thing_name),
            qos=mqtt.QoS.AT_LEAST_ONCE,
            callback=on_get_shadow_accepted)

        get_rejected_subscribed_future, _ = shadow_client.subscribe_to_get_shadow_rejected(
            request=iotshadow.GetShadowSubscriptionRequest(thing_name=thing_name),
            qos=mqtt.QoS.AT_LEAST_ONCE,
            callback=on_get_shadow_rejected)

        # Wait for subscriptions to succeed
        get_accepted_subscribed_future.result()
        get_rejected_subscribed_future.result()

        # The rest of the program runs asyncronously.

        # Issue request for shadow's current state.
        # The response will be received by the on_get_accepted() callback
        print("Requesting current shadow state...")
        publish_get_future = shadow_client.publish_get_shadow(
            request=iotshadow.GetShadowRequest(thing_name=thing_name),
            qos=mqtt.QoS.AT_LEAST_ONCE)

        # Ensure that publish succeeds
        publish_get_future.result()

        # Launch thread to handle user input.
        # A "daemon" thread won't prevent the program from shutting down.
        print("Launching thread to read user input...")
        user_input_thread = threading.Thread(target=user_input_thread_fn, name='user_input_thread')
        user_input_thread.daemon = True
        user_input_thread.start()

    except Exception as e:
        exit(e)

    # Wait for the finish (user types 'quit', or an error occurs)
    is_done.wait()


if __name__ == '__main__':
    args = parser.parse_args()
    shadow_handler(args.endpoint, args.cert, args.key, args.root_ca, args.thing_name)

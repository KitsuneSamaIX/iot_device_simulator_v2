from utils.config_loader import Config
from glob import glob
from uuid import uuid4
import json
from concurrent.futures import ThreadPoolExecutor

from provisioning import run_provisioning
from shadow import shadow_handler
from publogs import publish_device_logs

from awscrt import auth, io, mqtt, http
from awsiot import mqtt_connection_builder


if __name__ == '__main__':
    # set shadow to default values (for testing)
    with open('./shadow_defaults.json', 'r') as shadow_defaults:
        with open('./shadow.json', 'w') as shadow:
            shadow.write(shadow_defaults.read())

    # set config path
    CONFIG_PATH = 'config.ini'

    # get path for certs
    config = Config(CONFIG_PATH)
    config_parameters = config.get_section('SETTINGS')
    iot_endpoint = config_parameters['IOT_ENDPOINT']
    secure_cert_path = config_parameters['SECURE_CERT_PATH']
    root_ca = config_parameters['ROOT_CERT']

    # check if we already have a provisioned certificate and key
    #   the provisioned certificates and keys are saved locally with suffixes:
    #       *-provisioned-certificate.pem.crt
    #       *-provisioned-private.pem.key
    #   where * stands for a general prefix string
    provisioned_certs = glob(f'{secure_cert_path}/*-provisioned-certificate.pem.crt')
    provisioned_keys = glob(f'{secure_cert_path}/*-provisioned-private.pem.key')

    if not (len(provisioned_certs) > 0 and len(provisioned_keys) > 0):
        print("Missing provisioned certificates")
        choice = input("Would you like to run fleet provisioning to automatically claim certificates and create a "
                       "new thing?\nType 'y' to confirm or 'n' to abort: ")
        if choice == 'y':
            print("Executing fleet provisioning...")
            run_provisioning()
            # retrieve newly created certs
            provisioned_certs = glob(f'{secure_cert_path}/*-provisioned-certificate.pem.crt')
            provisioned_keys = glob(f'{secure_cert_path}/*-provisioned-private.pem.key')
        else:
            print("Quitting application...")
            quit(0)

    # set paths
    cert_path = provisioned_certs[0]
    key_path = provisioned_keys[0]
    root_ca_path = f'{secure_cert_path}/{root_ca}'

    # get thing name
    with open('provisioned_thing.json', 'r') as f:
        provisioned_thing_data = json.load(f)
        thing_name = provisioned_thing_data['thingName']

    # Establish mqtt connection with the provisioned certificates
    print("Establishing mqtt connection with the provisioned certificates...")

    # spin up resources
    event_loop_group = io.EventLoopGroup(1)
    host_resolver = io.DefaultHostResolver(event_loop_group)
    client_bootstrap = io.ClientBootstrap(event_loop_group, host_resolver)

    # set mqtt connection parameters
    mqtt_connection = mqtt_connection_builder.mtls_from_path(
        endpoint=iot_endpoint,
        cert_filepath=cert_path,
        pri_key_filepath=key_path,
        client_bootstrap=client_bootstrap,
        ca_filepath=root_ca_path,
        client_id=thing_name,
        clean_session=False,
        keep_alive_secs=6
    )

    print("Connecting to {} with client ID '{}'...".format(iot_endpoint, thing_name))

    connect_future = mqtt_connection.connect()

    # future.result() waits until a result is available
    connect_future.result()
    print("Connected!")

    # simulate device working
    with ThreadPoolExecutor(max_workers=5) as executor:
        print("Keeping shadow in sync...")
        executor.submit(shadow_handler, iot_endpoint, cert_path, key_path, root_ca_path, thing_name, thing_name, mqtt_connection)

        print("Publishing simulated device logs...")
        executor.submit(publish_device_logs, iot_endpoint, cert_path, key_path, root_ca_path, thing_name, thing_name, mqtt_connection)

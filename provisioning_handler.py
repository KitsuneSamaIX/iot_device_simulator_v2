# ------------------------------------------------------------------------------
# Demonstrates how to call/orchestrate AWS fleet provisioning services
#  with a provided bootstrap certificate (aka - provisioning claim cert).
#
# ------------------------------------------------------------------------------

from awscrt import io, mqtt, auth, http
from awsiot import mqtt_connection_builder
from utils.config_loader import Config
import time
import logging
import json 
import os
import asyncio
import glob
from uuid import uuid4


class ProvisioningHandler:

    def __init__(self, file_path):
        """Initializes the provisioning handler
        
        Arguments:
            file_path {string} -- path to your configuration file
        """
        #Logging
        logging.basicConfig(level=logging.ERROR)
        self.logger = logging.getLogger(__name__)
        
        #Load configuration settings from config.ini
        config = Config(file_path)
        self.config_parameters = config.get_section('SETTINGS')
        self.secure_cert_path = self.config_parameters['SECURE_CERT_PATH']
        self.iot_endpoint = self.config_parameters['IOT_ENDPOINT']	
        self.template_name = self.config_parameters['PRODUCTION_TEMPLATE']
        self.claim_cert = self.config_parameters['CLAIM_CERT']
        self.secure_key = self.config_parameters['SECURE_KEY']
        self.root_cert = self.config_parameters['ROOT_CERT']
    
        # Provisioning Template requests a serial number as a
        # seed to generate Thing names in IoTCore. Simulating here.
        #self.unique_id = str(int(round(time.time() * 1000)))
        # self.unique_id = "1234567-abcde-fghij-klmno-1234567abc-TLS350"
        self.unique_id = str(uuid4())

        # ------------------------------------------------------------------------------
        #  -- PROVISIONING HOOKS EXAMPLE --
        # Provisioning Hooks are a powerful feature for fleet provisioning. Most of the
        # heavy lifting is performed within the cloud lambda. However, you can send
        # device attributes to be validated by the lambda. An example is show in the line
        # below (.hasValidAccount could be checked in the cloud against a database). 
        # Alternatively, a serial number, geo-location, or any attribute could be sent.
        # 
        # -- Note: This attribute is passed up as part of the register_thing method and
        # will be validated in your lambda's event data.
        # ------------------------------------------------------------------------------

        self.primary_MQTTClient = None
        self.workflow_completed = False

    def core_connect(self):
        """ Method used to connect to connect to AWS IoTCore Service. Endpoint collected from config.
        
        """
        self.logger.info('##### CONNECTING WITH PROVISIONING CLAIM CERT #####')
        print('##### CONNECTING WITH PROVISIONING CLAIM CERT #####')

        event_loop_group = io.EventLoopGroup(1)
        host_resolver = io.DefaultHostResolver(event_loop_group)
        client_bootstrap = io.ClientBootstrap(event_loop_group, host_resolver)

        self.primary_MQTTClient = mqtt_connection_builder.mtls_from_path(
            endpoint=self.iot_endpoint,
            cert_filepath="{}/{}".format(self.secure_cert_path, self.claim_cert),
            pri_key_filepath="{}/{}".format(self.secure_cert_path, self.secure_key),
            client_bootstrap=client_bootstrap,
            ca_filepath="{}/{}".format(self.secure_cert_path, self.root_cert),
            on_connection_interrupted=self.on_connection_interrupted,
            on_connection_resumed=self.on_connection_resumed,
            client_id=self.unique_id,
            clean_session=False,
            keep_alive_secs=6)
        
        print("Connecting to {} with client ID '{}'...".format(self.iot_endpoint, self.unique_id))
        connect_future = self.primary_MQTTClient.connect()
        # Future.result() waits until a result is available
        connect_future.result()
        print("Connected!")

    def on_connection_interrupted(self, connection, error, **kwargs):
        print('connection interrupted with error {}'.format(error))

    def on_connection_resumed(self, connection, return_code, session_present, **kwargs):
        print('connection resumed with return code {}, session present {}'.format(return_code, session_present))

    def enable_response_monitor(self):
        """ Subscribe to pertinent IoTCore topics
        """
        template_rejected_topic = "$aws/provisioning-templates/{}/provision/json/rejected".format(self.template_name)
        certificate_rejected_topic = "$aws/certificates/create/json/rejected"
        
        template_accepted_topic = "$aws/provisioning-templates/{}/provision/json/accepted".format(self.template_name)
        certificate_accepted_topic = "$aws/certificates/create/json/accepted"

        subscribe_topics = [template_rejected_topic, certificate_rejected_topic, template_accepted_topic, certificate_accepted_topic]

        for mqtt_topic in subscribe_topics:
            print("Subscribing to topic '{}'...".format(mqtt_topic))
            mqtt_topic_subscribe_future, _ = self.primary_MQTTClient.subscribe(
                topic=mqtt_topic,
                qos=mqtt.QoS.AT_LEAST_ONCE,
                callback=self.on_message_callback)

            # Wait for subscription to succeed
            mqtt_topic_subscribe_result = mqtt_topic_subscribe_future.result()
            print("Subscribed with {}".format(str(mqtt_topic_subscribe_result['qos'])))

    def get_official_certs(self):
        """ Initiates an async loop/call to kick off the provisioning flow.

            Triggers:
               on_message_callback() providing the certificate payload
        """

        return asyncio.run(self.orchestrate_provisioning_flow())

    async def orchestrate_provisioning_flow(self):
        # Connect to core with provision claim creds
        self.core_connect()

        # Monitor response topics
        self.enable_response_monitor()

        # Make a publish call to topic to get official certs
        self.primary_MQTTClient.publish(
            topic="$aws/certificates/create/json",
            payload="{}",
            qos=mqtt.QoS.AT_LEAST_ONCE)
        time.sleep(1)

        # Wait until the workflow is completed
        while not self.workflow_completed:
            await asyncio.sleep(0)

    def on_message_callback(self, topic, payload, **kwargs):
        """ Callback Message handler responsible for workflow routing of msg responses from provisioning services.
        
        Arguments:
            payload {bytes} -- The response message payload.
        """
        print("Received message from topic '{}': {}".format(topic, payload))

        # Check if a request is rejected
        if (topic == "$aws/provisioning-templates/{}/provision/json/rejected".format(self.template_name) or
                topic == "$aws/certificates/create/json/rejected"):
            print("Failed provisioning")
            self.workflow_completed = True

        else:
            json_data = json.loads(payload)

            # A response has been recieved from the service that contains certificate data.
            if 'certificateId' in json_data:
                self.logger.info('##### SUCCESS. SAVING KEYS TO DEVICE! #####')
                print('##### SUCCESS. SAVING KEYS TO DEVICE! #####')
                self.assemble_certificates(json_data)

            # A response contains acknowledgement that the provisioning template has been acted upon.
            elif 'deviceConfiguration' in json_data:
                self.logger.info('##### CERT ACTIVATED AND THING {} CREATED #####'.format(json_data['thingName']))
                print('##### CERT ACTIVATED AND THING {} CREATED #####'.format(json_data['thingName']))
                with open('provisioned_thing.json', 'w') as f:
                    json.dump(json_data, f)

                print("##### ACTIVATED CREDENTIALS ({}, {}). #####".format(self.new_key_name, self.new_cert_name))
                print("##### CERT FILES SAVED TO {} #####".format(self.secure_cert_path))

                self.workflow_completed = True

            else:
                self.logger.info(json_data)

    def assemble_certificates(self, payload):
        """ Method takes the payload and constructs/saves the certificate and private key. Method uses
        existing AWS IoT Core naming convention.
        
        Arguments:
            payload {dict} -- Certificate data (as strings).

        Returns:
            ownership_token {string} -- proof of ownership from certificate issuance activity.
        """
        # Cert ID
        cert_id = payload['certificateId']
        self.new_key_root = cert_id[0:10]

        self.new_cert_name = '{}-provisioned-certificate.pem.crt'.format(self.new_key_root)
        # Create certificate
        f = open('{}/{}'.format(self.secure_cert_path, self.new_cert_name), 'w+')
        f.write(payload['certificatePem'])
        f.close()

        # Create private key
        self.new_key_name = '{}-provisioned-private.pem.key'.format(self.new_key_root)
        f = open('{}/{}'.format(self.secure_cert_path, self.new_key_name), 'w+')
        f.write(payload['privateKey'])
        f.close()

        # Extract/return Ownership token
        self.ownership_token = payload['certificateOwnershipToken']
        
        # Register newly aquired cert
        self.register_thing(self.unique_id, self.ownership_token)

    def register_thing(self, serial, token):
        """Calls the fleet provisioning service responsible for acting upon instructions within device templates.
        
        Arguments:
            serial {string} -- unique identifer for the thing. Specified as a property in provisioning template.
            token {string} -- The token response from certificate creation to prove ownership/immediate possession of the certs.
            
        Triggers:
            on_message_callback() - providing acknowledgement that the provisioning template was processed.
        """

        self.logger.info('##### CREATING THING ACTIVATING CERT #####')
        print('##### CREATING THING ACTIVATING CERT #####')
                
        register_template = {"certificateOwnershipToken": token, "parameters": {"SerialNumber": serial}}
        
        #Register thing / activate certificate
        self.primary_MQTTClient.publish(
            topic="$aws/provisioning-templates/{}/provision/json".format(self.template_name),
            payload=json.dumps(register_template),
            qos=mqtt.QoS.AT_LEAST_ONCE)
        time.sleep(2)

# -----------------------------------------
# Consuming sample, demonstrating how a device process would leverage the provisioning class.
#  The handler makes use of the asycio library and therefore requires Python 3.7.
#
#  Prereq's: 
#      1) A provisioning claim certificate has been cut from AWSIoT.
#	   2) A restrictive "birth" policy has been associated with the certificate.
#      3) A provisioning template was created to manage the activities to be performed during new certificate activation.
#	   4) The claim certificate was placed securely on the device fleet and shipped to the field. (along with root/ca and private key)
#  
#  Execution:
#      1) The paths to the certificates, and names of IoTCore endpoint and provisioning template are set in config.ini (this project)  
#	   2) A device boots up and encounters it's "first run" experience and executes the process (main) below.
# 	   3) The process instatiates a handler that uses the bootstrap certificate to connect to IoTCore.
#	   4) The connection only enables calls to the Foundry provisioning services, where a new certificate is requested.
#      5) The certificate is assembled from the response payload, and a foundry service call is made to activate the certificate.
#	   6) The provisioning template executes the instructions provided.
#      7) New certificates are saved locally, and can be stored/consumed as the application deems necessary.
#
#
# ------------------------------------------------------------------------------

from provisioning_handler import ProvisioningHandler
from utils.config_loader import Config


#Set Config path
CONFIG_PATH = 'config.ini'

config = Config(CONFIG_PATH)
config_parameters = config.get_section('SETTINGS')
secure_cert_path = config_parameters['SECURE_CERT_PATH']
bootstrap_cert = config_parameters['CLAIM_CERT']


# Used to kick off the provisioning lifecycle, exchanging the bootstrap cert for a
#  production certificate after being validated by a provisioning hook lambda.
def run_provisioning():

    provisioner = ProvisioningHandler(CONFIG_PATH)

    #Check for availability of bootstrap cert
    try:
        with open("{}/{}".format(secure_cert_path, bootstrap_cert)) as f:
            # Call super-method to perform aquisition/activation
            # of certs, creation of thing, etc.
            provisioner.get_official_certs()

    except IOError:
        print("### Bootstrap cert non-existent.")


if __name__ == "__main__":
    run_provisioning()

import sys
import urllib3
from opensearchpy import OpenSearch, RequestsHttpConnection, AWSV4SignerAuth
from botocore.credentials import Credentials
from dotenv import load_dotenv
import os

def create_OpenSearch_client():
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    sys.path.append('/opt/airflow/dags/Scripts')
    load_dotenv()
    host=os.getenv('host')
    region = 'us-east-1' # e.g. us-west-1

    # Specify your AWS access key and secret access key directly
    OpenSearch_aws_access_key = os.getenv('OpenSearch_aws_access_key')
    OpenSearch_aws_secret_key = os.getenv('OpenSearch_aws_secret_key')
    # Create a Credentials object
    credentials = Credentials(
        access_key=OpenSearch_aws_access_key,
        secret_key=OpenSearch_aws_secret_key
    )

    # Pass the Credentials object to AWSV4SignerAuth constructor
    auth = AWSV4SignerAuth(credentials, region)
    try:
        client = OpenSearch(
                hosts = [{'host': host, 'port': 443}],
                http_auth = auth,
                use_ssl = True,
                verify_certs = False,
                connection_class = RequestsHttpConnection
            )
    
    except Exception as e:
        raise(e)
    return client

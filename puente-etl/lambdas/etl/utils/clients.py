import boto3
from pymongo import MongoClient as MC
import utils.secretz as secretz

class Clients:
    #
    # Initialize AWS S3 Client
    #
    S3 = boto3.client(
        's3',
        # aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        # aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        aws_access_key_id=secretz.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=secretz.AWS_SECRET_ACCESS_KEY,
    )
    # S3_BUCKET_NAME = os.getenv('AWS_S3_BUCKET')
    # S3_OUTPUT_BUCKET = os.getenv('AWS_S3_OUTPUT_BUCKET')
    S3_BUCKET_NAME = secretz.AWS_S3_BUCKET
    S3_OUTPUT_BUCKET = secretz.AWS_S3_OUTPUT_BUCKET

    #
    # Initialize MongoDB Client & Connect to Database
    #
    # The name that Back4App gives its databases can be found
    # after the last slash in MongoDB URI string
    mongo_client = MC(secretz.DATABASE_URI)
    db_name = mongo_client.get_default_database().name
    MONGO = mongo_client[db_name]

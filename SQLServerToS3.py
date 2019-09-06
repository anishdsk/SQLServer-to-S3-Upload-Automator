import json
import boto3
import uuid
import pandas as pd
import pyodbc

# inputs
sqlServer = input("Enter the server name: ")
serverDatabase = input("Enter database name: ")
fileName = input("Create name of csv file that will contain the query's data " +
                    "with '.csv' suffix (name will be appended to additonal" +
                    "UUID hex characters for fast upload and retrieval): ")
query = input("Enter your database table(s) query statement: ")
bucketPrefix = input("Enter a prefix for the bucket name to be created " +
                        "(prefix will be added onto a randomly generated ID " +
                        "to make bucket name valid): ")

# connect to your Microsoft SQL Server
sqlConnection = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};'
                               f'Server={sqlServer};'
                               f'Database={serverDatabase};'
                               'Trusted_Connection=yes')

# open a file and query from table
with open(fileName, "w") as file:
    chunks = pd.read_sql_query(query, con=sqlConnection, chunksize=10000)

    for chunk in chunks: # read data in chunks to avoid most maximum RAM usage errors
        chunk.to_csv(file, index=False)

sqlConnection.close() # close connection

# boto3 resource()
s3_resource = boto3.resource('s3')

# function to create random bucket name to prevent any DNS compliance errors
def createBucketName(bucketPrefix):
    # generated bucket name must be between 3 and 64 characters long
    return ''.join([bucketPrefix, str(uuid.uuid4())])

# creates an s3 bucket with proper location configurations
def createBucket(bucket_prefix, s3Connection):
    session = boto3.session.Session()
    currentRegion = session.region_name
    bucketName = createBucketName(bucket_prefix)
    bucketResponse = s3_connection.createBucket(
        Bucket=bucketName,
        CreateBucketConfiguration={
        'LocationConstraint': currentRegion})
    print(bucketName, currentRegion)
    return bucketName, bucketResponse

# create bucket using resource
finalBucketName, response = create_bucket(
    bucket_prefix=bucketPrefix, s3Connection=s3_resource)

# generates fileName using a UUID prefix to maximize speed of file upload and retrieval
# different prefix means filename will be mapped to different partions rather than
# being cluttered in one map partition with other files having the same file name prefix
def createDataFile(fileName):
    # takes 4 chars of the number’s hex representation and appends it with your file name
    randomFileName = ''.join([str(uuid.uuid4().hex[:4]), fileName])
    return randomFileName

finalFileName = createDataFile(fileName)
# upload file to S3 bucket using an 'Object' instance and AES-256 encryption
s3_resource.Object(finalBucketName, finalFileName).upload_file(Filename = finalFileName,
                                                                ExtraArgs = {'ServerSideEncryption': 'AES256'})

# enables bucket versioning which can keep a record of your objects over time
def enableBucketVersioning(bucketName):
    bucketVersion = s3_resource.BucketVersioning(bucket_name)
    bucketVersion.enable()

enableBucketVersioning(finalBucketName)

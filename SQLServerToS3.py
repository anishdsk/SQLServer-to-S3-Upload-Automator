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

serverName = 'Server=' + sqlServer + ';'
databaseName = 'Database=' + serverDatabase + ';'

# connect to your Microsoft SQL Server
sqlConnection = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};',
                               serverName,
                               databaseName,
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
def createBucket(bucketPrefix, s3_connection):
    session = boto3.session.Session()
    currentRegion = session.region_name
    bucketName = createBucketName(bucketPrefix)
    bucketResponse = s3_connection.createBucket(
        Bucket=bucketName,
        CreateBucketConfiguration={
        'LocationConstraint': currentRegion})
    print(bucketName, currentRegion)
    return bucketName, bucketResponse

# create bucket using resource
firstBucketName, firstResponse = create_bucket(
    bucket_prefix=bucketPrefix, s3_connection=s3_resource)

# generates fileName using a UUID prefix to maximize speed of file upload and retrieval
# different prefix means filename will be mapped to different partions rather than
# being cluttered in one map partition with other files having the same file name prefix
def createTempFile(fileName):
    randomFileName = ''.join([str(uuid.uuid4().hex[:4]), fileName]) # takes 4 chars of the number’s hex representation and appends it with your file name
    return randomFileName

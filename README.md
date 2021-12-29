# SQLServer-to-S3-Upload-Automator

## Detailed Overview:
Takes data from your MS SQL Server database based on input query, converts it to CSV data (keeps a copy locally), and uploads it to an AWS S3 Bucket contigent on having an AWS IAM user with proper permissions configured, AWS CLI configured properly, and having the required Python packages installed. When running the script, after providing for asked inputs on the command line (CSV file name, server name, db name, etc.), this script takes care of of secure data transfer from on-premise to an S3 bucket. The script can be further automated by hardcoding in the required inputs and running it as a cron job or running it on AWS Lambda after a few modifcations.

## How to Run:

### Prerequisites:
      - Have an AWS account and an IAM user established with the "AmazonS3FullAccess" policy
      - Save your key ID and access key credentails which will be generated from creating the user
      - Setup AWS CLI with the created user credentials 
      
### Installing Dependencies (using pip):
      - Python should already be installed and added to PATH
      - "pip install json" (it may already be installed)
      - "pip install boto3"
      - "pip install uuid"
      - "pip install pandas"
      - "pip install pyodbc"

### Running:
      - Run the file
      - Provide all the inputs as it is asked on the commmand line
      - The queried data should be exported to a local CSV file
      - The queried data (CSV form) will also be automatically stored into a new S3 bucket
      - You can verify by going into S3 and checking

### Automating Options:
      - Can be slightly modified and run on AWS Lambda as a scheduled event
      - Cron job

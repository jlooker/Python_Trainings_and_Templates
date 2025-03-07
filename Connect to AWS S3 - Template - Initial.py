"""
    The "Connect to AWS S3 - Template" file showcases how to pull data from an AWS S3 Bucket, looping through all files within the AWS S3 Bucket, then loading the data into a Pandas DataFrame

    Data Pipeline Process:

        Step 1: Install Required Python Packages

        Step 2: Install Required Python Libraries

        Step 3: Setup the credentials to connect to the AWS S3 Bucket

        Step 4: Connect to the AWS S3 Bucket

        Step 5: Collect a list of file names for all files within the AWS S3 Bucket

        Step 6: Extract the data from the file(s) within the AWS S3 Bucket and load into a Pandas DataFrame

        Step 7: Consolidate all lists within the all_data list into 1 Pandas Dataframe

        Step 8: Select the columns to keep from the Pandas DataFrame
"""


##############################################################################################################
# Step 1: Install Required Python Packages
    # Install all of the require Python packages in order to perform the necessary actions within the Python notebook
        # This section is only used for tools that require you to install all of the necessary packages before each time the code is executed, such as Databricks
##############################################################################################################


# Install the boto3 Python package
    # Used to connect to AWS services, like S3 Bucket	
%pip install boto3

# Install the datetime Python package
    # Used when working with and manipulating dates and times	
%pip install datetime

# Install the pandas Python package
    # Used to store data in Series and DataFrames	
%pip install pandas

# Restart the kernel to use updated packages
    # Databricks specific command that is required to use any newly install Python packages listed above
%restart_python


##############################################################################################################
# Step 2: Install Required Python Libraries
    # Install all of the require Python libraries in order to perform the necessary actions within the Python notebook
##############################################################################################################


# Used when working with and manipulating dates and times
from datetime import datetime, timedelta

# Enables the ability to connect to multiple AWS services, such as S3 Bucket
import boto3

# Enables the ability to search for file names based on specified string pattern(s)
    # Resource: https://docs.python.org/3/library/fnmatch.html
import fnmatch

# Enables the ability to utilize DataFrames, which are 2 dimension data structures, such as a 2-dimension arrays or a tables with rows and columns
    # This Python library is already pre-installed within every Snowflake notebook, therefore does not need to be manually imported within this Snowflake notebook
        # The import statement is listed for dev purposes when building and testing in Visual Studio Code and commented out when executed in Snowflake notebook
import pandas as pd


##############################################################################################################
# Step 3: Setup the credentials to connect to the AWS S3 Bucket
##############################################################################################################


# The access ID assigned to the account connecting to the AWS service
s3_access_id = "<AWS_S3_Access_ID>"

# The access key assigned to the account connecting to the AWS service
s3_secret_access_key = "<AWS_S3_Access_Key>"

# The AWS service that you would like to connect to
aws_service = "s3"

# The AWS region that the AWS service is set to
aws_region_name = "<AWS_Region>"

# The AWS S3 Bucket that you would like to connect to
s3_bucket = "<AWS_S3_Bucket>"

# The report associated with the file within the AWS S3 Bucket
    # Available datasets assigned to the account connecting to the AWS service
file_name = "<File_Name>"

# The extension of the file within the AWS S3 Bucket
file_extension = "<File_Extension>"

# The SFTP file name, which is a consolidation of the following 3 parts:
    # report_name - The name of the Sysco report file
    # sftp_file_extension - The extension of the Olo report file
s3_file_name_pattern = f"*{file_name}*.{file_extension}"

# The list collect the name for all of the files within the S3 bucket
    # This will allow us to verify if the file that we would like to extract exists
        # If the file does not exist, the data pipeline will not try to extract the file
            # This will prevent the data pipeline from failing
s3_file_list = []

# The list will consolidate all S3 csv files
    # Consolidating all S3 csv files into a list, then converting the entire list into a DataFrame
    # prevents multiple DataFrames from being created in the for loop
        # This method is more effiecient and less computational than converting each dictionary into a DataFrame
        # and appending to one another, then consolidating all DataFrames into one
all_data = []

# The oldest date that you would like to pull data for
start_date = datetime(<4-Digit Year>, <1 or 2-digit Month>, <1 or 2-digit Day>)

# The date used to calulate the date variables below
today = datetime.now()

# Identifies how many times the for loop below will have to iterate for
    # 1 time per day that you would like to pull data for
days_passed = (today - start_date).days - <Number of day offset>


##############################################################################################################
# Step 4: Connect to the AWS S3 Bucket
##############################################################################################################


# Connect to the AWS S3 Bucket
    # The connection to the AWS S3 Bucket does not allow using the with function
        # The connection to the AWS S3 Bucket closes automatically and does not use the close() command
s3 = boto3.resource(
    service_name = aws_service
    ,region_name = aws_region_name
    ,aws_access_key_id = s3_access_id
    ,aws_secret_access_key = s3_secret_access_key
)


##############################################################################################################
# Step 5: Collect a list of file names for all files within the AWS S3 Bucket
##############################################################################################################


# Loop through each file within the AWS S3 Bucket
for s3_bucket_file in s3.Bucket(s3_bucket).objects.all():

    # Display all of the file names within the AWS S3 Bucket
        # Comment Out once the files have been verified
    # print()
    # print(s3_bucket_file.key)

    # Collect the name of each file within the AWS S3 Bucket
    s3_file_list.append(s3_bucket_file.key)

"""
# Display all of the files within the AWS S3 Bucket
    # Comment Out once the files have been verified
print()
print("AWS S3 Bucket Files:")
print()
print(s3_file_list)
"""


##############################################################################################################
# Step 6: Extract the data from the file(s) within the AWS S3 Bucket and load into a Pandas DataFrame
##############################################################################################################


# Loop through the S3 bucket, once per day that we would like to pull data for
for i in range(days_passed) and today >= datetime.strptime("<Oldest Date you want data for>", "%Y-%m-%d"):

    # The date that the GET request is being executed
        # The date the data pipeline is executed
    file_date = today.strftime("%Y%m%d")

    # The year part of the file name within the AWS S3 Bucket
    s3_file_year = (today - timedelta(days = 3)).strftime("%Y")

    # The month part of the file name within the AWS S3 Bucket
    s3_file_month = (today - timedelta(days = 3)).strftime("%m")

    # The day part of the file name within the AWS S3 Bucket
    s3_file_day = (today - timedelta(days = 3)).strftime("%d")

    # The file within the AWS S3 Bucket that you would like to extract
    s3_file_name = f"{file_name}.{file_extension}"

    # Verify if the desired file name exists within the AWS S3 Bucket
        # If the file exists within the AWS S3 Bucket, extaract the file
    if s3_file_name in s3_file_list:
        
        # Loop through each of the files that match the specified pattern, one at a time, within the SFTP file path, specified above
            # This ensures that the same operations are performed on each file, one at a time
        if fnmatch.fnmatch(s3_file_name, s3_file_name_pattern):

            # Extract the data from the file within the AWS S3 Bucket
            s3_file = s3.Bucket(s3_bucket).Object(s3_file_name).get()
            
            # ***** Choose one of the 2 df = pd.read_csv functions below *****
            
            # Load the data from s3_file into the df Pandas DataFrame as is
            df = pd.read_csv(
                s3_file['Body']
                ,sep = "<field_delimiter>"
                ,index_col = 0
            )
            
            # Load the SFTP file into the a Pandas DataFrame, specifying the column names
            df = pd.read_csv(
                s3_file['Body']
                ,index_col = 0
                ,sep = "<field_delimiter>"
                # the names variable is used to specify columns names, if they do NOT exist or if you want to rename them from what they currently are
                ,names = [
                    'column_1'
                    ,'column_2'
                    ,'column_N'
                ]
            )

            """
            # Verify if there is any data within the df Pandas DataFrame
                # Comment out once the data has been verified
            if not df.empty:

                print("The df Pandas DataFrame contains data:")
                print()
                print(df)

            else:

                print("The df Pandas DataFrame is empty")
            """
        
            # Append each S3 csv file into the all_data list to consolidate all of the S3 csv files that are being extracted from S3
            all_data.append(df)

    # Adjust the date value in the today variable to subtract 1 day from the variable
        # This allowas us to look at and extract the S3 csv file for the next previous day
            # 1 day will be subtracted from the today variable after each S3 csv file is extracted until the end of the for loop
                # This allows us to extract the S3 csv file for each day from the date set in the start_date variable until the current date - 3 days
    today = today - timedelta(days = 1)


##############################################################################################################
# Step 7: Consolidate all lists within the all_data list into 1 Pandas Dataframe
##############################################################################################################


# Concat all of the separate S3 csv files into a formal Pandas DataFrame
df_concat = pd.concat(all_data)

"""
# Verify if there is any data within the all_data Pandas DataFrame
    # Comment out once the data has been verified
print()
print("The df_concat Pandas DataFrame contains data:")
print()
print(df_concat)
"""


##############################################################################################################
# Step 8: Select the columns to keep from the Pandas DataFrame
##############################################################################################################


"""
# Display the columns within the df_concat Pandas DataFrame
    # This will allow you to identify the columns within the dataset
        # This allows you to be able to remove any unecessary columns, using df_subset below, from the dataset before loading into Snowflake
print()
print("Here are all of the columns within the df_concat Pandas DataFrame")
print()
print(df_concat.columns)
"""

# Verify if the df_subset Pandas DataFrame contains data
# Verify if the df_concat Pandas DataFrame contains data
    # If the df_subset Pandas DataFrame contains data, load the data into Snowflake
#if not df_subset.empty:
if not df_concat.empty:
    
    # Select the desired columns to keep from the df Pandas DataFrame
        # This method is used to remove any unwanted data that was extracted from the file within the AWS S3 Bucket
            # This is similar to the SELECT clause in SQL
    df_subset = df_concat[
        [
            '<Column_1>'
            ',<Column_2>'
            ,'<Column_...N>'
        ]
    ]

    """
    # Verify if there is any data within the df_subset Pandas DataFrame
        # Comment out once the data has been verified
    if not df_subset.empty:

        print('The df_subset Pandas DataFrame contains data:')
        print()
        print(df_subset)

    else:

        print('The df_subset Pandas DataFrame is empty')
    """


"""
# Display a message that informs the developer that the Python data pipeline has completed
    # Comment out once the entire data pipeline has completed successfully
print()
print("The <DATA_SOURCE_NAME> <DATASET_NAME> Data Load Notebook has completed successfully")
print()
"""

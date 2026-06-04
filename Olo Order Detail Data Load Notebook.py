The "Olo Order Detail Data Load Notebook" pulls Order Detail data from Olo and loads it into Snowflake, where it is then cleaned up to make it ready for reporting purposes.

Data Pipeline Process:
Step 1: Install Required Python Packages

Step 2: Install Required Python Libraries

Step 3: Setup the credentials to connect to Olo API and Azure Blob Storage

Step 4: Define the necessary functions to help create our headers and signatures for upcoming API requests

Step 5: Connect to Olo API via GET Request

Step 6: Obtain link to redirect URL of ZIP file location via POST request

Step 7: Obtain data file from the ZIP within redirect link

Step 8: Parse XML data to prepare for load into Snowflake

Step 9: Load the data from the df_items Pandas DataFrame into the Snowflake data warehouse

Step 10: Transform data in Snowflake to meet presentation requirements

Step 11: Load raw data as a .csv into our Azure Blob archive

Step 12: Mark the file as processed



Step 1: Install Required Python Packages
- Install all of the require Python packages in order to perform the necessary actions within the Python notebook
  - This section is only used for tools that require you to install all of the necessary packages before each time the code is executed, such as Databricks

# Install the datetime Python package
    # Used when working with and manipulating dates and times	
%pip install datetime

# Install the pandas Python package
    # Used to store data in Series and DataFrames	
%pip install pandas

# Install the snowflake-connector-python[pandas] Python package
    # Used to connect to Snowflake and utilize Pandas DataFrames	
%pip install snowflake-connector-python[pandas]

# Install the azure-storage-blob Python package
    # Used to connect to Azure Blob Storage
%pip install azure-storage-blob

# Install the pyarrow Python package
    # Used as a cross-language development platform for in-memory data
%pip install pyarrow

# Install the fastparquet Python package
    # Used to process parquet formatted data with Pandas
%pip install fastparquet

# Install the lxml Python package
    # Used to process xml formatted data with Pandas
%pip install lxml

# Install the xmltodict Python package
    # Used to convert xml data into dictionaries so we can parse and prepare for loading into a dataframe
%pip install xmltodict

# Restart the kernel to use updated packages
    # Databricks specific command that is required to use any newly install Python packages listed above
%restart_python



Step 2: Install Required Python Libraries

Install all of the require Python libraries in order to perform the necessary actions within the Python notebook

# Enables the ability to work with and manipulate dates and times
from datetime import datetime, timedelta

# Enables the ability to connect to a Rest API
import requests

# Enables the ability to work with zip files
from zipfile import ZipFile

# Enables ability to read nested xml files and load xml data into a pandas dataframe
import xml.etree.ElementTree as ET

#Enables ability to convert xml data into a python readable dictionary that can then be loaded into a dataframe
import xmltodict

# Enables reading and writing streams of data. "io" stands for input/output
    # This Python library is already pre-installed within every Snowflake notebook, therefore does not need to be manually imported within this Snowflake notebook 
import io

# Enables the ability to utilize DataFrames, which are 2 dimension data structures, such as a 2-dimension arrays or a tables with rows and columns
    # This Python library is already pre-installed within every Snowflake notebook, therefore does not need to be manually imported within this Snowflake notebook
        # The import statement is listed for dev purposes when building and testing in Visual Studio Code and commented out when executed in Snowflake notebook
import pandas as pd

# Enables the ability to connect to the Snowflake Data Warehouse
    # This Python library is already pre-installed within every Snowflake notebook, therefore does not need to be manually imported within this Snowflake notebook
        # The import statement is listed for dev purposes when building and testing in Visual Studio Code and commented out when executed in Snowflake notebook
import snowflake.connector

# Enables the ability to write data into Snowflake from Pandas DataFrames
    # This Python library is already pre-installed within every Snowflake notebook, therefore does not need to be manually imported within this Snowflake notebook
        # The import statement is listed for dev purposes when building and testing in Visual Studio Code and commented out when executed in Snowflake notebook
from snowflake.connector.pandas_tools import write_pandas

# Enables the ability to interact with the operating system. In particular, "access" allows for the use of uid/gid to check access to a pathway for a file
from os import access

# Provides hashing algorithms (like SHA256) used to create fixed-length hashes of data
import hashlib

# Implements HMAC (Hash-based Message Authentication Code for securely signing messages with a secret key)
import hmac

# Provides functions to encode/decode data in Base64 (commonly used to safely transmit binary data as text)
import base64

# This package is an Olo suggestion for formatting date/time within the GET and POST requests
from email.utils import formatdate

# Enables writing our sign-in message in the GET and POST requests
import json

#Enables functionality of our GET and POST functions
import requests

# Enables the ability to work and interact with time-related tasks, such as formatting time fields or inserting timestamps
    # This Python library is already pre-installed within every Snowflake notebook, therefore does not need to be manually imported within this Snowflake notebook
import time

# Enables the ability to work with mathematical functions
    # This Python library is already pre-installed within every Snowflake notebook, therefore does not need to be manually imported within this Snowflake notebook
import math

# Enables of use of default cryptographic backend, which is used for low-level crypto operations like key generation and encryption
from cryptography.hazmat.backends import default_backend

# Enables use of tools for converting cryptographic keys to/from formats for storage and transmission
from cryptography.hazmat.primitives import serialization

# Enables the ability to access and manipulate Azure Blob Storeage containers and blobs
from azure.storage.blob import BlobServiceClient



Step 3: Setup the credentials to connect to Olo API and Azure Blob Storage

#######################################################################################################################
# Olo API Variables
#######################################################################################################################

# Base URL that is first used to generate a Report ID, then will become part of the redirect URL
base_url = ""

# Appendage to base_url which directs us to the API endpoint for Order Exports
path_and_query = ""

# Olo API ID credential for GET, POST and DELETE requests
client_id = ""

# Olo API key credential for GET, POST and DELETE requests
client_secret = ""

# Initialize this variable as a place holder. This only gets populated if a successful connection is made to report data
report_file = None

#######################################################################################################################
# Azure Blob Variables
#######################################################################################################################

# The date used to calulate the date variables below
today = datetime.now()

# The date that identifies the start and end date for the data within the report
    #  The date should be the day before the data pipeline is executed
report_date = (today - timedelta(days = 1)).strftime("%Y-%m-%d")

# Account Name of the Azure Blob Storage environment
account_name = ""

# Account Key for the Azure Blob Storage environment
account_key = ""

# Connection String for the Azure Blob Storage environment, which is created from the Account Name and Account Key listed above
connection_string = f"DefaultEndpointsProtocol=https;AccountName={account_name};AccountKey={account_key};EndpointSuffix=core.windows.net"

# Name of the Azure Blob Storage archive container that the file(s) will be archived into, for future use if needed
archive_container_name = ""

# The dataset name for all records that will be included in the blob file
blob_file_name = ""

# The invoice date for all records that will be included in the blob file
    #  The date is formatted as YYYY_MM_DD
date_of_data = (today - timedelta(days = 1)).strftime("%Y%m%d")

# The extension of the blob file
blob_file_extension = "csv"

# The blob name, which is a consolidation of the following 3 parts:
    # report_name: The dataset name for all records that will be included in the blob file
    # date_of_data: The invoice date for all records that will be included in the blob file
    # blob_file_extension: The extension of the blob file
blob_name = f"{blob_file_name}_{date_of_data}.{blob_file_extension}"

#######################################################################################################################
# Snowflake Variables
#######################################################################################################################

# The Snowflake transient / raw schema name
transient_schema_name = ""

# The Snowflake transient / raw table name
transient_table_name = ""

# The Snowflake persisted / clean schema name
persisted_schema_name = ""

# The Snowflake transient / clean table name
persisted_table_name = ""

# The Snowflake presentation / cube schema name
presentation_schema_name = ""

# The Snowflake presentation / cube table name
presentation_table_name = ""

# Private key used to match to the public key assigned to the Snowflake service user
private_key = b'''-----BEGIN PRIVATE KEY-----
-----END PRIVATE KEY-----'''

print(date_of_data)



Step 4: Define the necessary functions to help create our headers and signatures for upcoming API requests
  
# This function is used to create the signature in the format that Olo requires for each of the requests
def create_signature(client_secret, client_id, http_verb, content_type, hashed_body, path_and_query, time_stamp):
        message_to_sign = f"{client_id}\n{http_verb}\n{content_type}\n{hashed_body}\n{path_and_query}\n{time_stamp}"
        hmac_sha256 = hmac.new(client_secret.encode('utf-8'), message_to_sign.encode('utf-8'), hashlib.sha256)
        return base64.b64encode(hmac_sha256.digest()).decode('utf-8')

# Olo requires the signature to be encrypyed in hash UTF-8
def hash_request_body(body):
        sha256_hasher = hashlib.sha256()
        sha256_hasher.update(body.encode('utf-8'))
        hash_bytes = sha256_hasher.digest()
        return base64.b64encode(hash_bytes).decode('utf-8') 



Step 5: Connect to Olo API via GET Request

This establishes an Olo API batch ID which identifies the location of the data

# Olo requires data retrieval via API to be handled in three separate requests:
        # GET: Access the meta data which describes each day's data via the batch ID and the day in which that batch was generated
                # The goal of this GET Response is to get the batch ID for the dataset we are loading
                # Typically holds the past 30 days of data
        # POST: Use the batch_Id from the GET response to construct a URL endpoint which directs to the zip file in which the batch's data resides
                # URL resembles the base_url + batch ID at the end
        # DELETE: Mark the batch_Id as processed and remove it from the download list


##############################################################################################################################
# GET: Access the meta data which describes each day's data via the batch ID and the day in which that batch was generated
        # The goal of this GET Response is to get the batch ID for the dataset we are loading
        # File will hold data for the date prior to date the batch was generated
        # Olo typically holds the past 30 days of data
##############################################################################################################################


##############################################################################################################################
# Variables used in the hash_request_body function
##############################################################################################################################

# Use empty string for GET requests as they do not have a body.
http_body = ""  

# Encrypted body of signature
hashed_body = hash_request_body(http_body)


##############################################################################################################################
# Variables used in the create_signature function
##############################################################################################################################

# Appendage to base_url which directs us to the API endpoint for Order Exports
path_and_query = "/v1.1/orderexports"

# Indicates within the signed message that we have intiated a GET request
http_verb = "GET"

# Use empty string for GET requests as they do not have a content type.
content_type = ""  

# Time stamp for signature
time_stamp = formatdate(timeval=None, localtime=False, usegmt=True)

# Use create_signature function to assemble our signature
signed_message = create_signature(client_secret, client_id, http_verb, content_type, hashed_body, path_and_query, time_stamp)


##############################################################################################################################
# Initiate the GET request to obtain list of batch_Ids and batch generation dates
##############################################################################################################################

with requests.get(

    # Concat base_url and path_and_query to construct full URL
    url = base_url + path_and_query
    # Headers for our GET request
    ,headers = {
        "Authorization": f"OloSignature {client_id}:{signed_message}",
        "Date": time_stamp
    }
) as get_batch_id_response:

    # ensures 200-level response
    if get_batch_id_response.status_code == 200:   

        # Convert the GET Response to json for parsing
        data = get_batch_id_response.json()

        # This checks our json converted data to ensure we have connected to the right meta data
        if "batches" not in data:
            raise KeyError("Expected 'batches' key not found found in API Response")

        # Derive variable that dynamically calculates date - 1 of day pipeline is being ran formatted as YYYYMMDD
        get_batch_date = (datetime.today() - timedelta(days=1)).strftime("%Y%m%d")

        # Print get_batch_date for QA purposes
        # print(get_batch_date)

        # Derive variable that stores the batch id for the "generated" value within the get_response list that matches our date variable above
        get_batch_id = None
        # Loops through all batch_Ids
        for batch in data["batches"]:
            # Parses the time stamp assigned to "generated" key in json data to only pull the YYYYMMDD section
                    # Then assigns that value to the generated_dt variable for upcoming loop check
            generated_dt = datetime.strptime(batch["generated"], "%Y%m%d %H:%M")

            # During loop of json data, if the parsed date value in the json data matches the date we want (typically the day before the pipeline is ran), loop breaks and grabs the batch_Id of that date
            if generated_dt.strftime("%Y%m%d") == get_batch_date:
                get_batch_id = batch["batchId"]
                break
            # Function then spits out the batch_Id we will use in the POST request

    
        # Print GET response text for QA purposes
        print(get_batch_id_response.text)

        # Print get_batch_id for QA purposes
        print(get_batch_id)
        batch_id = str(get_batch_id)





Step 6: Obtain link to redirect URL of ZIP file location via POST request

##############################################################################################################################
# POST: Use the batch_Id from the GET response to construct a URL endpoint 
        # URL resembles the base_url + batch ID at the end
        # URL directs to the zip file in which the batch's data resides
##############################################################################################################################

##############################################################################################################################
# Variables used in the hash_request_body function
##############################################################################################################################

# http body to be encrypted
http_body = json.dumps("")

# Encrypted body of signature
hashed_body = hash_request_body(http_body)


##############################################################################################################################
# Variables used in the create_signature function
##############################################################################################################################

# Appendage to base_url which directs us to the API endpoint for Order Exports
path_and_query = f"/v1.1/orderexports/{batch_id}"

# Indicates within the signed message that we have intiated a POST request
http_verb = "POST"

# Indicates the content type we are accessing 
content_type = ""

# Time stamp for signature
time_stamp = formatdate(timeval=None, localtime=False, usegmt=True)

# Use create_signature function to assemble our signature
signed_message = create_signature(client_secret, client_id, http_verb, content_type, hashed_body, path_and_query, time_stamp)


##############################################################################################################################
# Initiate the POST request which redirects to the zip file which contains the data we need
##############################################################################################################################

with requests.post(
    
    # Concat base_url and path_and_query to construct full URL
    url = base_url + path_and_query
    # Headers for our POST request
    ,headers = {
        "Authorization": f"OloSignature {client_id}:{signed_message}",
        "Date": time_stamp,
        "Content-Type": content_type,
        "User-Agent": "KKC_Olo_Orders_Export/1.0" 
    }
    ,data=http_body
    ,allow_redirects=False 

) as post_response:
        
    # QA Purposes only
    # print(signed_message)
    # QA Purposes only
    # print(headers)

    # post_response = requests.post(url, headers=headers, data=http_body)
    # print(post_response.text)
    if post_response.status_code == 301:
            report_link = post_response.headers['Location']
            print(f"Status code: {post_response.status_code}")
            print(report_link)
            # Extract the report from the URL
                # Report Format: .zip (gzip) file with .csv file enclosed
            report_file = requests.get(report_link)
    else:
            print(f"No 301 redirect found. Status code: {post_response.status_code}/n{post_response.text}")




Step 7: Obtain data file from the ZIP within redirect link

Step 8: Parse XML data to prepare for load into Snowflake
     
##############################################################################################################################
# Pull the ZIP file from the redirect link
    # Convert file into readable XML bytes
    # Parse XML into a "rows" list 
        # This enables the data to be read and loaded into a dataframe
    # Load data into a dataframe
##############################################################################################################################

if report_file is not None:
    # Collect the contents within the zip file
    zip_file =  ZipFile(io.BytesIO(report_file.content))

    # Collect all of the file names into a list
    file_list = zip_file.namelist()

    # Convert the file list into a string
    file_name = "".join(file_list)

    # Quick check to verify the file name
    # print()
    # print(f"File name: {file_name}")
    # print()

    # Open the XML file within the zip
        # Then read it is a byte file so python
    with zip_file.open(file_name) as xml_file:
        xml_bytes = xml_file.read()
        # print(xml_bytes[:500])

    # Parse the XML bytes into a nested Python dictionary
    doc = xmltodict.parse(xml_bytes)

    # Navigate to the order records in the XML
    orders = doc["exportbatch"]["orders"]["order"]

    # If there is only one order, xmltodict returns a dict instead of a list
    if isinstance(orders, dict):
        orders = [orders]

    # Ensure value is always treated as a list
    def ensure_list(x):
        if x is None:
            return []
        if isinstance(x, list):
            return x
        return [x]

    # Convert XML-derived values into Snowflake-safe strings
    def extract_xml_value(x):
        if x is None:
            return None

        if isinstance(x, (str, int, float, bool)):
            return str(x)

        if isinstance(x, dict):
            if "#text" in x:
                return x["#text"]

            if "@xsi:nil" in x and str(x["@xsi:nil"]).lower() == "true":
                return None

            return json.dumps(x, ensure_ascii=False)

        if isinstance(x, list):
            return json.dumps(x, ensure_ascii=False)

        return str(x)


    # Store flattened rows
    rows = []


    # Recursive function to flatten modifiers
    def add_modifier_rows(order_base, parent_product, modifiers, level=1, parent_modifier_name=None):

        for mod in ensure_list(modifiers):

            rows.append({
                **order_base,
                "product_name": extract_xml_value(parent_product.get("@name")),
                "product_quantity": extract_xml_value(parent_product.get("@quantity")),
                "product_base_cost": extract_xml_value(parent_product.get("@baseCost")),
                "product_pos_ref": extract_xml_value(parent_product.get("@posRef")),

                "line_type": "modifier",
                "modifier_level": str(level),
                "parent_modifier_name": extract_xml_value(parent_modifier_name),

                "item_name": extract_xml_value(mod.get("@name")),
                "item_quantity": extract_xml_value(mod.get("@quantity")),
                "item_cost": extract_xml_value(mod.get("@cost")),
                "item_pos_ref": extract_xml_value(mod.get("@posRef"))
            })

            nested_modifiers = (
                mod.get("modifiers", {}).get("modifier")
                if mod.get("modifiers") else None
            )

            if nested_modifiers:
                add_modifier_rows(
                    order_base=order_base,
                    parent_product=parent_product,
                    modifiers=nested_modifiers,
                    level=level + 1,
                    parent_modifier_name=mod.get("@name")
                )


    # Loop through orders
    for order in orders:

        # Only NON-PII order-level fields
        order_base = {
            "order_olo_ref": extract_xml_value(order.get("oloRef")),
            "order_pos_ref": extract_xml_value(order.get("posRef")),
            "store_name": extract_xml_value(order.get("storeName")),
            "store_ref": extract_xml_value(order.get("storeRef")),
            "store_utc_offset": extract_xml_value(order.get("storeUtcOffset")),

            "handoff_method": extract_xml_value(
                order.get("handoffMethod", {}).get("@method") if order.get("handoffMethod") else None
            ),

            "subtotal": extract_xml_value(order.get("subtotal")),
            "discount": extract_xml_value(order.get("discount")),
            "tax": extract_xml_value(order.get("tax")),
            "tip": extract_xml_value(order.get("tip")),
            "delivery_fee": extract_xml_value(order.get("deliveryFee")),
            "custom_fee_total": extract_xml_value(order.get("customFeeTotal")),
            "donations_total": extract_xml_value(order.get("donationsTotal")),
            "total": extract_xml_value(order.get("total")),

            "time_placed": extract_xml_value(order.get("timePlaced")),
            "time_closed": extract_xml_value(order.get("timeClosed")),
            "time_wanted": extract_xml_value(order.get("timeWanted")),
            "time_estimate": extract_xml_value(order.get("timeEstimate")),

            "source": extract_xml_value(order.get("source")),

            "ordering_provider_name": extract_xml_value(
                order.get("orderingProvider", {}).get("name") if order.get("orderingProvider") else None
            ),
            "ordering_provider_slug": extract_xml_value(
                order.get("orderingProvider", {}).get("slug") if order.get("orderingProvider") else None
            ),

            "external_reference": extract_xml_value(order.get("externalReference")),
            "ordered_from_fave": extract_xml_value(order.get("orderedFromFave")),
            "handoff": extract_xml_value(order.get("handoff")),

            "client_platform": extract_xml_value(order.get("clientPlatform")),

            "coupon_code": extract_xml_value(
                order.get("coupon", {}).get("couponCode") if order.get("coupon") else None
            ),
            "coupon_description": extract_xml_value(
                order.get("coupon", {}).get("description") if order.get("coupon") else None
            ),
            "coupon_amount": extract_xml_value(
                order.get("coupon", {}).get("amount") if order.get("coupon") else None
            ),

            "is_marketplace_facilitator": extract_xml_value(
                order.get("reconciliation", {}).get("isMarketplaceFacilitator") if order.get("reconciliation") else None
            )
        }


        # Extract products
        products = order.get("products", {}).get("orderProduct")


        for product in ensure_list(products):

            # Base product row
            rows.append({
                **order_base,
                "product_name": extract_xml_value(product.get("@name")),
                "product_quantity": extract_xml_value(product.get("@quantity")),
                "product_base_cost": extract_xml_value(product.get("@baseCost")),
                "product_pos_ref": extract_xml_value(product.get("@posRef")),

                "line_type": "product",
                "modifier_level": "0",
                "parent_modifier_name": None,

                "item_name": extract_xml_value(product.get("@name")),
                "item_quantity": extract_xml_value(product.get("@quantity")),
                "item_cost": extract_xml_value(product.get("@baseCost")),
                "item_pos_ref": extract_xml_value(product.get("@posRef"))
            })


            # Extract modifiers
            modifiers = (
                product.get("modifiers", {}).get("modifier")
                if product.get("modifiers") else None
            )

            if modifiers:
                add_modifier_rows(
                    order_base=order_base,
                    parent_product=product,
                    modifiers=modifiers
                )


    # Convert to DataFrame
    df_items = pd.DataFrame(rows)

    # Ensure column names are strings
    df_items.columns = df_items.columns.map(str)

    # Final cleanup to ensure all values are strings or null
    for col in df_items.columns:
        df_items[col] = df_items[col].apply(extract_xml_value)

    # Convert column names to UPPER CASE in order to prepare df for Snowflake load
    df_items.columns = [col.upper() for col in df_items.columns]

    # Preview the cleaned DataFrame
    display(df_items.head(15))



Step 9: Load the data from the df_items Pandas DataFrame into the Snowflake data warehouse

Step 10: Transform data in Snowflake to meet presentation requirements

##############################################################################################################################
# Load data into Snowflake and transform for reporting purposes
##############################################################################################################################

if len(df_items) != 0:
    # Loads the private key from the specified file path. If the key is encrypted, the password argument is used
    p_key = serialization.load_pem_private_key(
        private_key
        ,password = None    # Set a password if your private key is encrypted
        ,backend = default_backend()
    )
    
    # Converts the loaded private key into a byte string in PKCS8 DER format, which is required by the Snowflake connector
    pkb = p_key.private_bytes(
        encoding = serialization.Encoding.DER
        ,format = serialization.PrivateFormat.PKCS8
        ,encryption_algorithm = serialization.NoEncryption()
    )
    
    # The connection to the Snowflake data warehouse closes automatically after the with block is exited
        # Therefore, no need for the dw_conn.close() command
    with snowflake.connector.connect(
        # The username should be tied to a service account, rather than and specific individual
        user = "SERVICEADMIN"
        # The private key should is tied to a service account, rather than and specific individual
        ,private_key = pkb
        # The account consists of 3 parts separated by a decimal (".")
            # Part 1: Snowflake account identifier
            # Part 2: Snowflake cloud region
            # Part 3: Snowflake cloud provider
        ,account = "xl70960.east-us-2.azure"
        # The Snowflake warehouse that will be used to write the data into Snowflake
        ,warehouse = "PRD_WH_OLO_ELT"
        # The Snowflake database where the data will be written into
        ,database = "DB_KKF_MAIN"
    ) as dw_conn:
    
    
        ####################################################################################################
        # Load the data from the df Pandas DataFrame into the Snowflake data warehouse table
        ####################################################################################################
    
        
        write_pandas(
            conn = dw_conn
            ,df = df_items
            ,table_name = transient_table_name
            ,schema = transient_schema_name
            # When set to False, no new table is created in Snowflake from the df Pandas DataFrame
                # If set to True, a new table will be created in Snowflake from the df Pandas DataFrame
            ,auto_create_table = False
            # When set to False, no quotes are added to each column value
                # When set to True, quotes are added to each column value
                    # If the data already contains quotes, set to False
                        # Quotes can be added to column values if working with csv files, which ensures the csv data is not parsed incorrecty
            ,quote_identifiers = True
            # The default value is False,
            # which appends that data from the df Pandas DataFrame to the end of the existing Snowflake table
                # When overwrite and auto_create_table are both set to True,
                # the Snowflake table is truncated before the data from the df Pandas DataFrame is loaded into the Snowflake table
                    # When overwrite is set to True and auto_create_table is set to False,
                    # the Snowflake table is dropped and then recreated with the data from the df Pandas DataFrame
            ,overwrite = True
        )
        
    
        ####################################################################################################
        # Establish a cursor
        ####################################################################################################
    
    
        # Setup a cursor in order to execute SQL queries to retrieve data from the Snowflake data warehouse
        with dw_conn.cursor() as cur:
        
        
            ####################################################################################################
            # Test the connection to Snowflake
                # Comment out once the connection has been verified
            ####################################################################################################
    
            
            """
            # Write and execute the SQL query that will return the current version of the Snowflake data warehouse
                # Identifying the current version of the Snowflake data warehouse ensures a successful connection to the Snowflake data warehouse
            cur.execute('''
                SELECT current_version()
            ''')
    
            # Fetch the first row/record from the SQL query above
            one_row = cur.fetchone()
            
            # Display the first row/record from the SQL query above
            print(one_row[0])
            """
    
    
            ####################################################################################################
            # Assign the proper values for the following Snowflake object
                # Role
                # Warehouse
                # Database
            ####################################################################################################
    
            
            # Assume the SYSADMIN role
            cur.execute("""
                USE ROLE SYSADMIN;
            """)
    
            # Assume the PRD_WH_OLO_ELT ELT Production Warehouse
            cur.execute("""
                USE WAREHOUSE PRD_WH_OLO_ELT;
            """)
    
            # Assume the DB_KKF_MAIN database
            cur.execute("""
                USE DATABASE DB_KKF_MAIN;
            """)
            
    
            ####################################################################################################
            # Insert the data pipeline execution metadata into the DB_KKF_MAIN.AUTOMATION.TASK_LIST table
            ####################################################################################################
    
            
            # Verify whether or not the TASK_OLO_ORDER_DETAIL_DATA_LOADS_DAILY_9_AM_EST task exists within the DB_KKF_MAIN.AUTOMATION.TASK_LIST table in Snowflake
                # Update the TASK_OLO_ORDER_DETAIL_DATA_LOADS_DAILY_9_AM_EST task record within the DB_KKF_MAIN.AUTOMATION.TASK_LIST table in Snowflake if it already exists
                # Insert the TASK_OLO_ORDER_DETAIL_DATA_LOADS_DAILY_9_AM_EST task record into the DB_KKF_MAIN.AUTOMATION.TASK_LIST table in Snowflake if it does NOT already exist
            cur.execute("""
                MERGE INTO DB_KKF_MAIN.AUTOMATION.TASK_LIST T
                    
                    USING
                    (
                        -- Assign the necessary values for each column
                        SELECT
                            'TASK_OLO_ORDER_DETAIL_DATA_LOADS_DAILY_9_AM_EST' AS TASK_NAME
                            ,'Loads all Olo order detail files into Snowflake then makes the necessary transformations to prepare the data for business use' AS TASK_DESCRIPTION
                            ,'Daily' AS TASK_FREQUENCY
                            ,'Sun - Sat' AS TASK_DAY_OF_WEEK
                            ,'9:00 AM EST' AS TASK_TIME_OF_DAY
                            ,NULL AS TASK_PREDECESSOR_NAME
                            ,TO_DATE(CONVERT_TIMEZONE('America/New_York' , CURRENT_DATE())) AS TASK_LAST_RUN_START_DATE
                            ,TO_TIME(CONVERT_TIMEZONE('America/New_York', CURRENT_TIMESTAMP())) AS TASK_LAST_RUN_START_TIME_IN_EST
                            ,NULL AS TASK_LAST_RUN_END_DATE
                            ,NULL AS TASK_LAST_RUN_END_TIME_IN_EST
                            ,NULL AS TASK_LAST_RUN_DURATION_IN_SECONDS
                    ) S
                    ON T.TASK_NAME = S.TASK_NAME
                    
                    -- Update the TASK_OLO_ORDER_DETAIL_DATA_LOADS_DAILY_9_AM_EST task record within the DB_KKF_MAIN.AUTOMATION.TASK_LIST table in Snowflake
                    -- Set the TASK_LAST_RUN_START_DATE & TASK_LAST_RUN_START_TIME_IN_EST with the current date and time in EST that the TASK started
                    WHEN MATCHED
                        
                        THEN UPDATE SET
                            T.TASK_LAST_RUN_START_DATE = S.TASK_LAST_RUN_START_DATE
                            ,T.TASK_LAST_RUN_START_TIME_IN_EST = S.TASK_LAST_RUN_START_TIME_IN_EST
                    
                    -- Insert the TASK_OLO_ORDER_DETAIL_DATA_LOADS_DAILY_9_AM_EST task record into the DB_KKF_MAIN.AUTOMATION.TASK_LIST table in Snowflake
                    WHEN NOT MATCHED
                        
                        THEN INSERT VALUES
                        (
                            S.TASK_NAME
                            ,S.TASK_DESCRIPTION
                            ,S.TASK_FREQUENCY
                            ,S.TASK_DAY_OF_WEEK
                            ,S.TASK_TIME_OF_DAY
                            ,S.TASK_PREDECESSOR_NAME
                            ,S.TASK_LAST_RUN_START_DATE
                            ,S.TASK_LAST_RUN_START_TIME_IN_EST
                            ,S.TASK_LAST_RUN_END_DATE
                            ,S.TASK_LAST_RUN_END_TIME_IN_EST
                            ,S.TASK_LAST_RUN_DURATION_IN_SECONDS
                        )
                ;
            """)
            
    
            ####################################################################################################
            # Truncate the Snowflake DB_KKF_MAIN.OLO_CLEAN.OLO_ORDER_DETAIL table
            ####################################################################################################
    
            
            # Truncate DB_KKF_MAIN.OLO_CLEAN.OLO_ORDER_DETAIL table so the data can be replace with updated data
            cur.execute(f"""
                TRUNCATE TABLE DB_KKF_MAIN.{persisted_schema_name}.{persisted_table_name};
            """)
            
    
            ####################################################################################################
            # Transform and load the Invoice data into the Snowflake DB_KKF_MAIN.PERSISTED.DOORDASH_ORDER_DETAIL table
            ####################################################################################################
    
            
            # Extract the raw data from the DB_KKF_MAIN.OLO_RAW.OLO_ORDER_DETAIL table
            # Transform the raw data to make it more useable for reporting purposes
            # Load the transformed data into the DB_KKF_MAIN.OLO_CLEAN.OLO_ORDER_DETAIL table
            cur.execute(f"""
                INSERT INTO DB_KKF_MAIN.{persisted_schema_name}.{persisted_table_name}
                
                    SELECT
                        CASE
                            WHEN ORDER_OLO_REF IS NOT NULL
                                THEN UPPER(TRIM(ORDER_OLO_REF))
                                ELSE 'UNKNOWN'
                            END AS ORDER_OLO_REF
                        ,CASE
                            WHEN STORE_NAME IS NOT NULL
                                THEN UPPER(TRIM(STORE_NAME))
                                ELSE 'UNKNOWN'
                            END AS STORE_NAME
                        ,CASE
    	                    WHEN STORE_REF IS NOT NULL
    	                        THEN CONCAT(REPEAT('0', 6 - LEN(UPPER(TRIM(STORE_REF)))),UPPER(TRIM(STORE_REF)))
                                ELSE 'UNKNOWN'
                            END AS STORE_REF
                        ,CASE
                            WHEN STORE_UTC_OFFSET IS NOT NULL
                                THEN UPPER(TRIM(STORE_UTC_OFFSET))
                                ELSE 'UNKNOWN'
                            END AS STORE_UTC_OFFSET
                        ,CASE
                            WHEN HANDOFF_METHOD IS NOT NULL
                                THEN UPPER(TRIM(HANDOFF_METHOD))
                                ELSE 'UNKNOWN'
                            END AS HANDOFF_METHOD
                        ,CASE
                            WHEN SUBTOTAL IS NOT NULL
                                THEN TO_NUMBER(TRIM(SUBTOTAL),10,2)
                                ELSE 0
                            END AS SUBTOTAL
                        ,CASE
                            WHEN DISCOUNT IS NOT NULL
                                THEN TO_NUMBER(TRIM(DISCOUNT),10,2)
                                ELSE 0
                            END AS DISCOUNT
                        ,CASE
                            WHEN TAX IS NOT NULL
                                THEN TO_NUMBER(TRIM(TAX),10,2)
                                ELSE 0
                            END AS TAX
                        ,CASE
                            WHEN TOTAL IS NOT NULL
                                THEN TO_NUMBER(TRIM(TOTAL),10,2)
                                ELSE 0
                            END AS TOTAL
                        ,CASE
                            WHEN TIME_CLOSED IS NOT NULL
                                THEN TO_DATE(TRIM(SPLIT_PART(TIME_CLOSED,'T',1)))
                                ELSE '1900-01-01'
                            END AS ORDER_DATE
                        ,CASE
                            WHEN TIME_PLACED IS NOT NULL
                                THEN TO_TIME(TRIM(SPLIT_PART(TIME_PLACED,'T',2)))
                                ELSE '00:00:00'
                            END AS TIME_PLACED
                        ,CASE
                            WHEN TIME_CLOSED IS NOT NULL
                                THEN TO_TIME(TRIM(SPLIT_PART(TIME_CLOSED,'T',2)))
                                ELSE '00:00:00'
                            END AS TIME_CLOSED
                        ,CASE
                            WHEN TIME_WANTED IS NOT NULL
                                THEN TO_TIME(TRIM(SPLIT_PART(TIME_WANTED,'T',2)))
                                ELSE '00:00:00'
                            END AS TIME_WANTED
                        ,CASE
                            WHEN TIME_ESTIMATE IS NOT NULL
                                THEN TO_NUMBER(TRIM(TIME_ESTIMATE),5,0)
                                ELSE 0
                            END AS TIME_ESTIMATE
                        ,CASE
                            WHEN SOURCE IS NOT NULL
                                THEN UPPER(TRIM(SOURCE))
                                ELSE 'UNKNOWN'
                            END AS SOURCE
                        ,CASE
                            WHEN ORDERING_PROVIDER_NAME IS NOT NULL
                                THEN UPPER(TRIM(ORDERING_PROVIDER_NAME))
                                ELSE 'UNKNOWN'
                            END AS ORDERING_PROVIDER_NAME
                        ,CASE
                            WHEN EXTERNAL_REFERENCE IS NOT NULL
                                THEN UPPER(TRIM(EXTERNAL_REFERENCE))
                                ELSE 'UNKNOWN'
                            END AS EXTERNAL_REFERENCE
                        ,CASE
                            WHEN HANDOFF IS NOT NULL
                                THEN UPPER(TRIM(HANDOFF))
                                ELSE 'UNKNOWN'
                            END AS HANDOFF
                        ,CASE
                            WHEN CLIENT_PLATFORM IS NOT NULL
                                THEN UPPER(TRIM(CLIENT_PLATFORM))
                                ELSE 'UNKNOWN'
                            END AS CLIENT_PLATFORM
                        ,CASE
                            WHEN COUPON_CODE IS NOT NULL
                                THEN UPPER(TRIM(COUPON_CODE))
                                ELSE 'UNKNOWN'
                            END AS COUPON_CODE
                        ,CASE
                            WHEN COUPON_DESCRIPTION IS NOT NULL
                                THEN UPPER(TRIM(COUPON_DESCRIPTION))
                                ELSE 'UNKNOWN'
                            END AS COUPON_DESCRIPTION
                        ,CASE
                            WHEN COUPON_AMOUNT IS NOT NULL
                                THEN TO_NUMBER(TRIM(COUPON_AMOUNT),10,2)
                                ELSE 0
                            END AS COUPON_AMOUNT
                        ,CASE
                            WHEN PRODUCT_NAME IS NOT NULL
                                THEN UPPER(TRIM(PRODUCT_NAME))
                                ELSE 'UNKNOWN'
                            END AS PRODUCT_NAME
                        ,CASE
                            WHEN PRODUCT_QUANTITY IS NOT NULL
                                THEN TO_NUMBER(TRIM(PRODUCT_QUANTITY),5,0)
                                ELSE 0
                            END AS PRODUCT_QUANTITY
                        ,CASE
                            WHEN PRODUCT_BASE_COST IS NOT NULL
                                THEN TO_NUMBER(TRIM(PRODUCT_BASE_COST),10,2)
                                ELSE 0
                            END AS PRODUCT_BASE_COST
                        ,CASE
                            WHEN PRODUCT_POS_REF IS NOT NULL
                                THEN UPPER(TRIM(PRODUCT_POS_REF))
                                ELSE 'UNKNOWN'
                            END AS PRODUCT_POS_REF
                        ,CASE
                            WHEN LINE_TYPE IS NOT NULL
                                THEN UPPER(TRIM(LINE_TYPE))
                                ELSE 'UNKNOWN'
                            END AS LINE_TYPE
                        ,CASE
                            WHEN MODIFIER_LEVEL IS NOT NULL
                                THEN UPPER(TRIM(MODIFIER_LEVEL))
                                ELSE 'UNKNOWN'
                            END AS MODIFIER_LEVEL
                        ,CASE
                            WHEN PARENT_MODIFIER_NAME IS NOT NULL
                                THEN UPPER(TRIM(PARENT_MODIFIER_NAME))
                                ELSE 'NOT APPLICABLE'
                            END AS PARENT_MODIFIER_NAME
                        ,CASE
                            WHEN ITEM_NAME IS NOT NULL
                                THEN UPPER(TRIM(ITEM_NAME))
                                ELSE 'UNKNOWN'
                            END AS ITEM_NAME
                        ,CASE
                            WHEN ITEM_QUANTITY IS NOT NULL
                                THEN TO_NUMBER(TRIM(ITEM_QUANTITY),5,0)
                                ELSE 0
                            END AS ITEM_QUANTITY
                        ,CASE
                            WHEN ITEM_COST IS NOT NULL
                                THEN TO_NUMBER(TRIM(ITEM_COST),10,2)
                                ELSE 0
                            END AS ITEM_COST
                        ,CASE
                            WHEN ITEM_POS_REF IS NOT NULL
                                THEN UPPER(TRIM(ITEM_POS_REF))
                                ELSE 'UNKNOWN'
                            END AS ITEM_POS_REF
                            
                    FROM DB_KKF_MAIN.{transient_schema_name}.{transient_table_name}
                ;
            """)
            
    
            ####################################################################################################
            # Merge the Invoice data into the Snowflake DB_KKF_MAIN.DOORDASH.FACT_ORDER_DETAIL table
                # The merge statement ensures that no duplicate records get loaded into the DB_KKF_MAIN.DOORDASH.FACT_ORDER_DETAIL table
                # as this will skew any reports that utilize the Olo Product Breakdown Data
            ####################################################################################################
    
            
            # Extract the raw data from the DB_KKF_MAIN.PERSISTED.DOORDASH_ORDER_DETAIL table
            # Transform the raw data to make it more useable for reporting purposes
            # Load the transformed data into the DB_KKF_MAIN.DOORDASH.FACT_ORDER_DETAIL table
            cur.execute(f"""
                INSERT INTO DB_KKF_MAIN.{presentation_schema_name}.{presentation_table_name}
                
                    SELECT *
                    
                    FROM DB_KKF_MAIN.{persisted_schema_name}.{persisted_table_name}
                ;
            """)
            
    
            ####################################################################################################
            # Update the data pipeline execution metadata in the DB_KKF_MAIN.AUTOMATION.TASK_LIST table
            ####################################################################################################
    
            
            # Update the TASK_OLO_ORDER_DETAIL_DATA_LOADS_DAILY_9_AM_EST task record within the DB_KKF_MAIN.AUTOMATION.TASK_LIST table in Snowflake
            # Set the TASK_LAST_RUN_END_DATE & TASK_LAST_RUN_END_TIME_IN_EST with the current date and time in EST that the TASK started
            cur.execute("""
                UPDATE DB_KKF_MAIN.AUTOMATION.TASK_LIST
    
                    SET
                        TASK_LAST_RUN_END_DATE = TO_DATE(CONVERT_TIMEZONE('America/New_York' , CURRENT_DATE()))
                        ,TASK_LAST_RUN_END_TIME_IN_EST = TO_TIME(CONVERT_TIMEZONE('America/New_York', CURRENT_TIMESTAMP()))
                    
                    WHERE TASK_NAME = 'TASK_OLO_ORDER_DETAIL_DATA_LOADS_DAILY_9_AM_EST'
                ;
            """)
    
            '''
            # Update the TASK_OLO_ORDER_DETAIL_DATA_LOADS_DAILY_9_AM_EST task record within the DB_KKF_MAIN.AUTOMATION.TASK_LIST table in Snowflake
            # Set the TASK_LAST_RUN_DURATION_IN_SECONDS updated value from taking the difference between the task start date and time and task end date and time
            cur.execute("""
                UPDATE DB_KKF_MAIN.AUTOMATION.TASK_LIST
    
                    SET TASK_LAST_RUN_DURATION_IN_SECONDS = DATEDIFF
                    (
                        SECOND
                        ,TASK_LAST_RUN_START_TIME_IN_EST
                        ,TASK_LAST_RUN_END_TIME_IN_EST
                    )
                    
                    WHERE TASK_NAME = 'TASK_OLO_ORDER_DETAIL_DATA_LOADS_DAILY_9_AM_EST'
                ;
            """)
            '''
            
            # Load the updated TASK_OLO_ORDER_DETAIL_DATA_LOADS_DAILY_9_AM_EST task record into the DB_KKF_MAIN.AUTOMATION.TASK_RUN_HISTORY table in Snowflake that keeps history of all task runs
            cur.execute("""
                INSERT INTO DB_KKF_MAIN.AUTOMATION.TASK_RUN_HISTORY
    
                    SELECT *
                    
                    FROM DB_KKF_MAIN.AUTOMATION.TASK_LIST
                    
                    WHERE TASK_NAME = 'TASK_OLO_ORDER_DETAIL_DATA_LOADS_DAILY_9_AM_EST'
                ;
            """)

else:
    print(f"Dataframe is empty. No data to load.")



Step 11: Load raw data as a .csv into our Azure Blob archive

# Verify if there is any data within the all_data list
    # Comment out once the data has been verified
if len(df_items) != 0:

    ####################################################################################################
    # Establish a Blob Service Client
        # Connect to the Azure Blob Storage account specified above
    ####################################################################################################


    # Establish the BlobServiceClient in order to interact with Azure Blob Storage at the account level
        # The connection to the BlobServiceClient closes automatically after the with block is exited
                # Therefore, no need for the blob_service_client.close() command
    with BlobServiceClient.from_connection_string(connection_string) as blob_service_client:


            ####################################################################################################
            # Establish a Container Client
                # Connect to the Azure Blob Storage archive container specified above
            ####################################################################################################


            # Establish a second ContainerClient in order to interact with the Azure Blob Storage archive container, specified above
            with blob_service_client.get_container_client(archive_container_name) as archive_container_client:


                ####################################################################################################
                # Establish a Blob Client
                    # Connect to the Azure Blob Storage archive blob file
                ####################################################################################################


                # Establish a second BlobClient in order to interact with the archive file within the Azure Blob Storage archive container, specified above
                with archive_container_client.get_blob_client(blob_name) as archive_blob_client:
                    

                    ####################################################################################################
                    # Load the data within the formatted Pandas DataFrame into the Azure Blob Storage archive blob file
                    ####################################################################################################


                    # Create a new blob file, or overwrite an existing blob file, within the Azure Blob Storage archive container
                        # Set overwrite = False, if you do not want to overwrite existing blob files that contain the same name as the file you are currently trying to create
                    archive_blob_client.upload_blob(
                        df_items.to_csv(index = False)
                        ,overwrite = True
                    )



Step 12: Mark the file as processed within Olo API

##############################################################################
        # Mark the original xml file as processed for Olo to remove
##############################################################################

if len(df_items) != 0:

        # Appendage to base_url which directs us to the API endpoint for Order Exports
        path_and_query = f"/v1.1/orderexports/{get_batch_id}"
        # Concat base_url and path_and_query to construct full URL
        url = base_url + path_and_query
        # Indicates within the signed message that we have intiated a DELETE request
        http_verb = "DELETE"
        # Indicates the content type we are accessing 
        content_type = ""
        # http body to be encrypted
        http_body = json.dumps("")
        # Encrypted body of signature
        hashed_body = hash_request_body(http_body)
        # Time stamp for signature
        time_stamp = formatdate(timeval=None, localtime=False, usegmt=True)
        # Use create_signature function to assemble our signature
        signed_message = create_signature(client_secret, client_id, http_verb, content_type, hashed_body, path_and_query, time_stamp)

        # Headers for our POST request
        headers = {
            "Authorization": f"OloSignature {client_id}:{signed_message}",
            "Date": time_stamp,
            "Content-Type": content_type,
            "User-Agent": "KKC_Olo_Orders_Export/1.0"  
        }
        del_response = requests.delete(url, headers=headers, data=http_body, allow_redirects=False)
        if del_response.status_code == 200:
                print(del_response.status_code)
                print(del_response.text)       
        else:
                print(f"DELETE request failed: {del_response.status_code}")
                print(del_response.text)
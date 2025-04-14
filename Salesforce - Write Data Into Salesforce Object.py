"""
    Resources:

        - https://pypi.org/project/simple-salesforce/

        - https://simple-salesforce.readthedocs.io/en/latest/
"""


"""
    Notes:

        - Need Modify All permission
            - Still have the Read Only profile
"""


####################################################################################################
# Identify required libraries
####################################################################################################


# Enables the ability to utilize DataFrames, which are 2 dimension data structures, such as a 2-dimension arrays or a tables with rows and columns
    # This Python library is already pre-installed within every Snowflake notebook, therefore does not need to be manually imported within this Snowflake notebook
        # The import statement is listed for dev purposes when building and testing in Visual Studio Code and commented out when executed in Snowflake notebook
import pandas as pd

# Enables the ability to connect to Salesforce
    # Documentation for simple_salesforce: https://pypi.org/project/simple-salesforce/
from simple_salesforce import Salesforce


####################################################################################################
# Setup Salesforce Credentials
####################################################################################################


# The URL that is used to identify the Salesforce environment that you wish to connect to
sf_instance_url = "https://krispykrunchychicken.my.salesforce.com/"

# The username of the account that will be used to connect to Salesforce
    # The username should be tied to a service account, rather than and specific individual
sf_user = "dataengineering@krispykrunchy.com"

# The password of the account that will be used to connect to Salesforce
    # The password should be tied to a service account, rather than and specific individual
sf_password = "KKF_DE_2025#"

# The security token of the account that will be used to connect to Salesforce
sf_scecurity_token = "JrsUfWNsY01t6wuvAFNgAg9hH"

# The consumer key tied to the Salesforce environment that you wish to connect to
sf_consumer_key = "3MVG9l2zHsylwlpS60YhvCSnXSO0S4D3g9ZDaKt1aEO1PNW.ne8bh59hBqdZNGTOuGw6cuL.bwND8qQDiQleb"

# The consumer secret tied to the Salesforce environment that you wish to connect to
sf_consumer_secret = "77A0DFC387DC2594B6F1099F8090BF75294577934F7FFB4AABCF33174D2145C1"

# The session ID that is used to authentication the Salesforce connection
sf_session_id = ""


####################################################################################################
# Setup Salesforce Connection
####################################################################################################


# Connect to Salesforce using the credentials specified above
sf = Salesforce(
    instance_url = sf_instance_url
    ,username = sf_user
    ,password = sf_password
    ,security_token = sf_scecurity_token
    ,consumer_key = sf_consumer_key
    ,consumer_secret = sf_consumer_secret
    ,session_id = sf_session_id
)


####################################################################################################
# Update Existing Records in Salesforce
####################################################################################################


# Identify the records and fields to be updated
updated_data = [
    {
        'Id': '<Object_Id_Value>'
        ,'<Field_Name>': '<Field_Value>'
    }
    ,{
        'Id': '<Object_Id_Value>'
        ,'<Field_Name>': '<Field_Value>'
    }
]

# Update all existing records listed in the updated_data list above
sf.bulk.Account.update(
    # Data that will be used to update existing records within the Salesforce object, based on the object ID
    updated_data
    # Number of records to update at a time
    ,batch_size = 10000
    # Set the concurrency mode, to allow records to run in parallel
    ,use_serial = True
)


# Update a single existing record
sf.Account.update(
    '<Object_Id_Value>'
    ,{
        '<Field_Name>': '<Field_Value>'
        ,'<Field_Name>': '<Field_Value>'
    }
)
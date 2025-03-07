"""
    Reference Material:
        - Youtube Video: https://www.youtube.com/watch?v=nPQFUgsk6Oo&t=283s
        - Website: https://satvasolutions.medium.com/salesforce-intwith-python-a-step-by-step-guide-40f9dca7e29b
"""


"""
    The "Connect to Salesforce - Template" file showcases how to pull data from Salesforce, then loading the data into a Pandas DataFrame

    Data Pipeline Process:

        Step 1: Install Required Python Packages

        Step 2: Install Required Python Libraries

        Step 3: Setup the credentials to connect to Salesforce

        Step 4: Connect to Salesforce

        Step 5: Query and Extract the data from the Salesforce object

        Step 6: Load the data from the SOQL query into a Pandas DataFrame
"""


##############################################################################################################
# Step 1: Install Required Python Packages
    # Install all of the require Python packages in order to perform the necessary actions within the Python notebook
##############################################################################################################


# Install the datetime Python package
    # Used when working with and manipulating dates and times	
%pip install datetime

# Install the simple_salesforce Python package
    # Used to connect to Salesforce environments	
%pip install simple_salesforce

# Install the pandas Python package
    # Used to store data in Series and DataFrames	
%pip install pandas

# Restart the kernel to use updated packages
    # Databricks specific command that is required to use any newly install Python packages listed above
%restart_python


####################################################################################################
# Step 2: Install Required Python Libraries
    # Install all of the require Python libraries in order to perform the necessary actions within the Python notebook
####################################################################################################


# Enables the ability to connect to Salesforce
    # Documentation for simple_salesforce: https://pypi.org/project/simple-salesforce/
from simple_salesforce import Salesforce

# Enables the ability to utilize DataFrames, which are 2 dimension data structures, such as a 2-dimension arrays or a tables with rows and columns
import pandas as pd


##############################################################################################################
# Step 3: Setup the credentials to connect to Salesforce
##############################################################################################################


# The URL that is used to identify the Salesforce environment that you wish to connect to
sf_instance_url = "<Salesforce_URL>"

# The username of the account that will be used to connect to Salesforce
    # The username should be tied to a service account, rather than and specific individual
sf_user = "<Salesforce_Username>"

# The password of the account that will be used to connect to Salesforce
    # The password should be tied to a service account, rather than and specific individual
sf_password = "<Salesforce_Password>"

# The security token of the account that will be used to connect to Salesforce
sf_security_token = "<Salesforce_Security_Token>"

# The consumer key tied to the Salesforce environment that you wish to connect to
sf_consumer_key = "<Salesforce_Consumer_Key>"

# The consumer secret tied to the Salesforce environment that you wish to connect to
sf_consumer_secret = "<Salesforce_Consumer_Secret>"

# (Optional) The session ID that is used to authentication the Salesforce connection
sf_session_id = "<Session_ID>"


##############################################################################################################
# Step 4: Connect to Salesforce
##############################################################################################################


# Connect to Salesforce using the credentials specified above
sf = Salesforce(
    instance_url = sf_instance_url
    ,username = sf_user
    ,password = sf_password
    ,security_token = sf_scecurity_token
    ,consumer_key = sf_consumer_key
    ,consumer_secret = sf_consumer_secret
    # Optional
    ,session_id = sf_session_id
)


##############################################################################################################
# Collect a list of file names for all files within Salesforce
    # This will help identify what data is available to extract within the Salesforce Object
        # Comment out once the field names have been identified
##############################################################################################################


# Create a dictionary to collect Salesforce object metadata
desc_sf_obj = sf.Account.describe()

# Create a list to collect all of the field names from the desc_sf_obj dictionary
field_names = [field['name'] for field in desc_sf_obj['fields']]

"""
# Display all of the field names within the field_names list
    # Comment out once all of the field names have been identified
print()
print("field_names results:")
print()
print(field_names)
"""


####################################################################################################
# Step 5: Query and Extract the data from the Salesforce object
####################################################################################################


# Write the SOQL query that will extract the necessary data from the Salesforce object
soql_query = """
    SELECT
        <Column_1>
        ,<Column_2>
        ,<Column_N>
    
    FROM <Object_Name>
"""

# Load the data from the SOQL query into the soql_result variable
soql_result = sf.query_all(soql_query)

"""
# Display the data within the soql_result variable
    # Comment out once the data has been verified
print()
print("soql_result results:")
print()
print(soql_result)
"""


####################################################################################################
# Step 6: Load the data from the SOQL query into a Pandas DataFrame
####################################################################################################


# Load the data from the soql_result variable above into a Pandas DataFrame
df = pd.DataFrame(soql_result.get('records'))

"""
# Display the data within the df Pandas DataFrame
    # Comment out once the data has been verified
print()
print("df results:")
print()
print(df)
"""

# Drop the attributes column, as we do not wish to load this column into Snowflake
df = df.drop(columns = ["attributes"])

"""
# Display the updated data within the Pandas DataFrame with the attributes column removed
    # Comment out once the data has been verified
print()
print("df results with the attributes column removed:")
print()
print(df)
"""
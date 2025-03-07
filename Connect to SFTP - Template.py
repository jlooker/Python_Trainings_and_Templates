"""
    The "Connect to SFTP - Template" file showcases how to pull data from a SFTP, looping through all files within the SFTP, then loading the data into a Pandas DataFrame

    Data Pipeline Process:

        Step 1: Install Required Python Packages

        Step 2: Install Required Python Libraries

        Step 3: Setup the credentials to connect to the SFTP

        Step 4: Connect to the SFTP

        Step 5: Establish a SFTP client object

            Step 6: Extract the data from the file within the SFTP File Path

                Step 7: Identify the necessary SFTP file path file(s)
                
                    Step 8: Connect to the SFTP file path file
                
                        Step 9: Load the data from the SFTP file path file into a Pandas DataFrame
                
                        Step 10: Convert the Pandas DataFrame to the proper format
                
        Step 11: Consolidate all csv files in the all_date List into 1 Pandas Dataframe
        
        Step 12: Load Pandas DataFrame into Target Location
        
        Step 13: Delete the SFTP file path file
"""


##############################################################################################################
# Step 1: Install Required Python Packages
    # Install all of the require Python packages in order to perform the necessary actions within the Python notebook
        # This section is only used for tools that require you to install all of the necessary packages before each time the code is executed, such as Databricks
##############################################################################################################


# Install the datetime Python package
    # Used when working with and manipulating dates and times	
%pip install datetime

# Install the paramiko Python package
    # Used to connect to SFTP environments	
%pip install paramiko

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


# Used when working with and manipulating dates and times
from datetime import datetime, timedelta

# Enables the ability to connect to the SFTP
    # pysftp not longer being maintained, therefore should use paramiko instead
import paramiko

# Enables the ability to search for file names based on specified string pattern(s)
    # Resource: https://docs.python.org/3/library/fnmatch.html
import fnmatch

# Enables the ability to utilize DataFrames, which are 2 dimension data structures, such as a 2-dimension arrays or a tables with rows and columns
import pandas as pd


####################################################################################################
# Step 3: Setup the credentials to connect to the SFTP
    # Open a SSH connection and connect to the SFTP
        # SSH uses encryption to establish a secure connection between a client and a server, such as a SFTP server
####################################################################################################


# Establish the SSHClient in order to interact with the SFTP SSH server
ssh_client = paramiko.SSHClient()

# Set the policy for handling unknown host keys to autmoatically add the host keys to the known hosts file
ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

# Set the SFTP file path that contains the source file(s)
    # "." is the default file path used if no file path is specified
        # The "." default file path should be used if the file(s) do NOT have a file path / directory
            # i.e. the file(s) are not stored in a folder
sftp_file_path = "<Path_To_SFTP_File(s)>"

# The name of the SFTP file
file_name = "<File_Name>"

# The extension of the SFTP file
file_extension = "<File_Extension>"

# The pattern of the SFTP file name, which is a consolidation of the following 2 parts:
    # file_name - The name of the SFTP file
    # file_extension - The extension of the SFTP file
sftp_file_name_pattern = f"*{file_name}*.{file_extension}"

# The list will consolidate all SFTP file names
    # Consolidating all SFTP files into a list, then converting the entire list into a Pandas DataFrame
    # prevents multiple Pandas DataFrames from being created in the for loop
        # This method is more effiecient and less computational than converting each dictionary into a Pandas DataFrame
        # and appending to one another, then consolidating all Pandas DataFrames into one
all_data = []

# The list will consolidate the name of each SFTP file that has been loaded into the target location
loaded_file_list = []


####################################################################################################
# Step 4: Connect to the SFTP
####################################################################################################


# Connect to the SFTP SSH server
ssh_client.connect(
    hostname = "<Host_Name>"
    ,port = "<Port_Number>"
    ,username = "<Username>"
    ,password = "<Password>"
    ,look_for_keys = False
)


####################################################################################################
# Step 5: Establish a SFTP client object
####################################################################################################


# Establish a SFTP client object, which enables the ability to interact with the SFTP
    # The connection to the SFTP closes automatically after the with block is exited
        # Therefore, no need for the sftp.close() command
with ssh_client.open_sftp() as sftp:


    ####################################################################################################
    # Step 6: Extract the data from the file within the SFTP File Path
    ####################################################################################################


    # Create a list of files that are contained within the SFTP file path, specified above
        # Loop through each of the files, one at a time, within the SFTP file path, specified above
            # This ensures that the same operations are performed on each file, one at a time
    for sftp_file in sftp.listdir(sftp_file_path):
        
        
        ####################################################################################################
        # Step 7: Identify the necessary SFTP file path file(s)
            # Loop through the necessary SFTP file path file(s)
        ####################################################################################################
        
        
        # Loop through each of the files that match the specified pattern, one at a time, within the SFTP file path, specified above
            # This ensures that the same operations are performed on each file, one at a time
        if fnmatch.fnmatch(sftp_file, sftp_file_name_pattern):
            
            # Append the SFTP file name to the loaded_file_list list
            loaded_file_list.append(sftp_file)


            ###################################################################################################
            # Step 8: Connect to the SFTP file path file
                # Open the SFTP file path file
            ####################################################################################################


            # Open the SFTP file in order to interact with the file within the SFTP file path, specified above
                # The file is automatically closed after the with block is exited
                    # Therefore no need for the remote_file.close() command
            with sftp.open(file) as sftp_file:
        

                ####################################################################################################
                # Step 9: Load the data from the SFTP file path file into a Pandas DataFrame
                ####################################################################################################


                # Prefetches the data within the SFTP file in the background as soon as the file is open
                # Prefetch helps speed up the process of pulling the data from the SFTP file and loading it to the df Pandas DataFrame
                sftp_file.prefetch()

                # Load the SFTP file into the a Pandas DataFrame
                df = pd.read_csv(
                    sftp_file
                    ,sep = "|"
                    # the names variable is used to specify columns names, if they do NOT exist or if you want to rename them from what they currently are
                    ,names = [
                        '<Column_1>'
                        ,'<Column_2>'
                        ,'<Column_N>'
                    ]
                )

                """
                # Verify if there is any data within the df Pandas DataFrame
                    # Comment out once the data has been verified
                if not df.empty:

                    print()
                    print("The df Pandas DataFrame contains data:")
                    print()
                    print(df)

                else:

                    print("The df Pandas DataFrame is empty")
                """

                # Append each SFTP file path file into the all_data list to consolidate all of the SFTP file path files that are being extracted from the SFTP file path
                all_data.append(df)
        

            ####################################################################################################
            # Step 10: Convert the Pandas DataFrame to the proper format
            ####################################################################################################
        

            # Convert the Pandas DataFrame to a CSV string in a new Pandas DataFrame
                # This enables the ability to write the data within the Pandas DataFrame into a Azure Blob Storage file
            df_string = df.to_csv(index = False)

            # Display the data within the new Pandas DataFrame
                # Remove once the data has been verified
            print(df_string)


####################################################################################################
# Step 11: Consolidate all csv files in the all_date List into 1 Pandas Dataframe
####################################################################################################


# Verify if there is any data within the all_data list
    # Comment out once the data has been verified
if len(all_data) != 0:

    # Concat all of the separate S3 csv files into a formal Pandas DataFrame
    df_concat = pd.concat(all_data)

    # Change the data type of all columns within the df_concat Pandas DataFrame to string
    df_concat = df_concat.astype(str)

    """
    # Verify if there is any data within the all_data Pandas DataFrame
        # Comment out once the data has been verified
    print()
    print("The df_concat Pandas DataFrame contains data:")
    print()
    print(df_concat)
    """


####################################################################################################
# Step 12: Load Pandas DataFrame into Target Location
####################################################################################################


# Insert code to load Pandas DataFrame into target location


####################################################################################################
# Step 13: Delete the SFTP file path file
####################################################################################################


# Establish a SFTP client object, which enables the ability to interact with the SFTP
    # The connection to the SFTP closes automatically after the with block is exited
        # Therefore, no need for the sftp.close() command
with ssh_client.open_sftp() as sftp:

    """
    # Display all of the files within the kkfolosftp SFTP
        # Comment out once the files have been verified
    print()
    print("sftp_file_path files:")
    print()
    print(sftp.listdir(sftp_file_path))
    print()
    """

    ##############################################################################################################
    # Delete the file within the SFTP File Path
    ##############################################################################################################


    # Loop through the SFTP file path, once per file within the SFTP file path 
    for loaded_sftp_file_name in sftp.listdir(sftp_file_path):

        # Verify if the file being deleted matches the list of files that have been loaded into the data warehouse
            # Only delete loaded files that have been loaded into the data warehouse
        if loaded_sftp_file_name in loaded_file_list:

            # Delete the SFTP file after it has been loaded into the destination, so that the file does not get loaded again
            sftp.remove(f"{sftp_file_path}/{loaded_sftp_file_name}")
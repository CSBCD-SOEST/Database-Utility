# file: Database_Insert.py
# name: Steven Chen
# date: 09/18/2014
# mod : 09/22/2014

# Re-purposed for the Kuykendall data collection project. Table name(s) still
# need to be entered under login information.

import DatabaseUtility
import os
import shutil
import sys
import time

#set the working directory of this script to its path directory
#this is to help crontab figure out where the supporting files are
print sys.argv[0]
if os.path.dirname(sys.argv[0]) == '':
    pass
else:
    os.chdir(os.path.dirname(sys.argv[0]))

#define the login information
database = "radiantpanels"
user     = ""
password = ""
table    = ""
table_fields = ["datetime","channel","value","sensor_id"]

#define the folder locations for the script
temp_folder     = "temperature"
flow_folder     = "CalibratedFlow"
data_folder     = "processed"
archive_folder  = "archive"
config_filename = "config"

with open(config_filename, 'r') as config_file:
    password = config_file.read()

#define the database object
A = DatabaseUtility.DatabaseUtility(database, user, password)

#welcome the user
print "===================================="
print "Starting Comfort Study Upload Script"
#print instructions for the user
print "Push <ctrl> + <c> to quit!"
print "===================================="
print
#loop is replaced with cron
#target files within the processed directory
for target in os.listdir(flow_folder):
    #targets only CSV formatted files
    if target.endswith(".csv"):
        #form the path to the CSV file
        target = os.path.join(flow_folder, target)
        #let user know which file is being uploaded
        print "Uploading... " + target
        #clear check table
        A.truncate("check_table")
        #insert target file into the database
        table_fields = ["datetime","channel","value","sensor_id"]
        A.copy_data_into_database(target, "check_table", table_fields)
        #merge
        A.merge("check_table", "radiant_panels", table_fields, "value")
        #move finished file into an archive
        shutil.move(target, archive_folder)
        #without this line, the server hosting the database will run out
        #of virtual memory
        #sleep for 30 seconds
        time.sleep(5)
#let the user know that the script is going into stand-by
print "Shutting down..."

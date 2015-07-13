#!/usr/bin/env python
'''
This script is a database utility for the ADASEED software.
'''

import datetime
import time
import os
import psycopg2
import logging
import csv

__author__ = "Christian A. Damo"
__copyright__ = "Copyright 2014 School of Architecture, University of Hawaii at Manoa"
__credits__ = ["Christian A. Damo", "Reed Shinsato"]
__version__ = "0.01"
__maintainer__ = "Eileen Peppard"
__email__ = "epeppard@hawaii.edu"
__status__ = "Prototype"

logging.basicConfig(filename='ADASEED_log',level=logging.DEBUG,format='%(asctime)s %(message)s')


#this is another comment

class DatabaseUtility:
    '''
    This is a package for interfacing with the postgres database
    through the python. This is basically an API that controls actions
    done to the database utilizing the psycopg2 package.
    '''
    def __init__(self,dbname='postgres',user='postgres',password='postgres'):
        DatabaseInfo = 'dbname='+dbname+' user='+user+' password='+password
        self.conn = psycopg2.connect(DatabaseInfo)
        self.cur = self.conn.cursor()

    def truncate(self, tableName):
        '''
        Clear all data entries for specified table.
        '''
        try:
            self.cur.execute("BEGIN")
            sql = "TRUNCATE " + str(tableName) + ";"
            self.cur.execute(sql)
            self.conn.commit()
        except Exception, e:
            print e

    def merge(self, source_table, destination_table, header, value_column):
        try:
            self.cur.execute("BEGIN")
            sql = 'INSERT INTO "public".' + destination_table + ' SELECT '
            for i, column in enumerate(header):
                sql = sql + '"public".' + source_table + '.' + column
                if i != (len(header) - 1):
                    sql = sql + ', '

            sql = sql + ' FROM "public".' + destination_table + \
                  ' FULL OUTER JOIN "public".' + source_table + ' ON '
            for i, column in enumerate(header):
                sql = sql + '"public".' + destination_table + '.' + column + \
                      ' = "public".' + source_table + '.' + column
                if i != (len(header) - 1):
                    sql = sql + ' AND '

            sql = sql + ' WHERE "public".' + destination_table + '.' + \
                  value_column + ' IS NULL;'
            # print sql
            self.cur.execute(sql)
            self.conn.commit()

        except Exception, e:
            print e

    def copy_data_into_database(self, insertFilename, tableName, \
                                table_fields = []):
        '''
        given: a file name of a csv file and a table name in string form
        return: nothing, but inputs the data into a psql database on the local
                local computer into the specified table name (faster)
        '''
        #open csv to check for a header
        with open(insertFilename, 'r') as insertFile:
            header_flag = csv.Sniffer().has_header(insertFile.read(1024))
            #setup the csv reader object
            reader = csv.reader(insertFile)
            #automatically pick up the field names from the header of the CSV
            if header_flag == True and table_fields == []:
                table_fields =  list(reader)[0]

            #header row is missing or the table fields are provided
            else:
                pass

        try:
            self.cur.execute("BEGIN;")
            sql = "COPY FROM "+tableName+" ("

            #dynamically expand the insert command for all table fields
            field_counter = 1
            for field in table_fields:
                if field_counter == len(table_fields):
                    sql = sql + field
                else:
                    sql = sql + field + ','
                field_counter = field_counter + 1

            sql = sql + ") FROM '" + os.path.abspath(insertFilename) + "'"
            sql = sql + " DELIMITER ',' CSV;"

            #this executes the command stored in sql
            #it acts like you just typed the command line
            #at the psql prompt
            sql = "COPY check_table FROM '" + os.path.abspath(insertFilename) + "' " + "DELIMITER ',' CSV;"
            self.cur.execute(sql)
            #this commits the command in the psql
            #similar to hitting enter
            self.conn.commit()
        except Exception, e:
            print e


    # Modified the code so that the inserting process is dynamic to any amount
    # of columns presented in the data. The insert function will now take a list
    # of table field names present in your table. If you do not want to provide
    # a list, then the function will attempt to extract the field names from the
    # data itself and use that to push into the database.
    def insert_data_into_database(self, insertFilename, tableName, \
                                  table_fields = []):
        '''
        given: a file name of a csv file and a table name in string form
        return: nothing, but inputs the dta into a psql database on the
                local comuter into the specified table name
        '''
        #open csv to check for a header
        with open(insertFilename, 'r') as insertFile:
            header_flag = csv.Sniffer().has_header(insertFile.read(1024))

        #reopen the csv file
        insertFile = open(insertFilename,'r')
        #setup the csv reader object
        reader = csv.reader(insertFile)

        #automatically pick up the field names from the header of the CSV
        if header_flag == True and table_fields == []:
            table_fields =  list(reader)[0]

        #header row is missing or the table fields are provided
        else:
            print "copy error"
            pass


        print "Inserting data into the "+tableName+" now ..."

        #this block of code feeds into the database one entry at a time
        #if there is an error during insertion eg, violates primary key
        #constraints, then it counts the error and outputs it to the
        #user.
        error = 0
        for row in reader:
            try:
                #keeps a savepoint so that in the event there
                #is an error, then it will roll back in the
                #exception block of code
                self.cur.execute("BEGIN;")
                self.cur.execute("SAVEPOINT my_savepoint;")
                #create the command name here
                sql = "INSERT INTO "+tableName+" ("

                #dynamically expand the insert command for all table fields
                field_counter = 1
                for field in table_fields:
                    if field_counter == len(table_fields):
                        sql = sql + field
                    else:
                        sql = sql + field + ','
                    field_counter = field_counter + 1

                sql = sql + ") VALUES ('"

                #dynamically expand the insert command for all data fields
                column_counter = 1
                for column in row:
                    if column_counter == len(row):
                        sql = sql + column + "'"
                    else:
                        sql = sql + column + "','"
                    column_counter = column_counter + 1

                sql = sql + ");"
                #this executes the command stored in sql
                #it acts like you just typed the command line
                #at the psql prompt
                self.cur.execute(sql)
                #this commits the command in the psql
                #similar to hitting enter
                self.conn.commit()
            except Exception, e:
                self.cur.execute("ROLLBACK TO SAVEPOINT my_savepoint;")
                logging.warning(str(row[0])+","+row[1]+","+str(row[2])+str(e)+"\n")
                #keeps record of how many errors occured
                error = error + 1
        #output to the user stating at what time the insertion
        #finished and how many errors there were
        print "At "+str(datetime.datetime.now()) + " there were "+str(error)+" error(s)"
        print "please refer to the 'ADASEED_log' file for more information.\n"
        insertFile.close()

    def create_table(self,tableName):
        '''
        given: a string
        return: nothing but creates a table in the psql database with
               the string as the name
        '''
        #this checks to see if the table already exists in the database
        #if the specified table name exists then it ends the method
        #with a return of 1
        if self.check_table_exists(tableName) == True:
            return 1

        try:
            #otherwise the program creates the command string
            sql = 'CREATE TABLE '+tableName+'( "timestamp" timestamp without time zone NOT NULL, sensor_id character varying NOT NULL, value double precision, CONSTRAINT '+tableName+'_prim_key PRIMARY KEY ("timestamp", sensor_id)) WITH( OIDS=FALSE);'
            #types it out at the psql prompt
            self.cur.execute(sql)
            #then hits enter to commit the command
            self.conn.commit()
            #We then change the owner of the table to postgres
            #for security purposes, most likely it defaults to
            #postgres, but this is just to make sure
            sql = 'ALTER TABLE '+tableName+' OWNER TO postgres;'
            #again execute is as if you're typing thte command at
            #the psql prompt
            self.cur.execute(sql)
            #commits it as if you're hitting enter at the end of
            #the command
            self.conn.commit()
        except Exception, e:
            #in the event there's some error with the above code
            #the software will notify the user that it failed
            print "did not create table "+tableName


    def close_conn(self):
        '''
        given: nothing
        return: nothing but closes the connection, this is just for good
        house keeping practice
        '''
        self.conn.close()

    def check_table_exists(self,tableName):
        '''
        given: a string of the name of the table desired
        return: a True or False to say if the specified table name
               does indeed exist
        '''
        #queries the database to see if the table is within the list
        #of tables that's in the database's schema
        self.cur.execute("select exists(select * from information_schema.tables where table_name=%s)", (tableName,))
        #return True or False if the specified table name exists or not
        return self.cur.fetchone()[0]

#!/usr/bin/env python3

#
#  Libraries and Modules
#
import configparser
import os 
import sys 
import pathlib
import boto3
import A2Functions
import pathlib
import readline
#
#  Find AWS access key id and secret access key information
#  from configuration file
#
config = configparser.ConfigParser()
config.read("S5-S3.conf")
aws_access_key_id = config['default']['aws_access_key_id']
aws_secret_access_key = config['default']['aws_secret_access_key']

print ( "Welcome to the AWS DynamoDB CLI" )

try:
#  Establish an AWS session
    session = boto3.Session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )

#  Set up client and resources
    dynamo_cli = session.client('dynamodb', region_name="ca-central-1")
    dynamo_res = session.resource('dynamodb', region_name="ca-central-1")
except:
    print ( "You could not be connected to your DynamoDB storage\nPlease review procedures for authenticating your account on AWS DynamoDB" )

print("You are now connected to your DynamoDB storage!")
userInput = 0
while (True):
    print("Enter an option's number:\n1. Create Tables & Load CSV\n2. Delete All tables\n3. Manually Add, Delete or Update a record\n4. Dump to Terminal from Table\n5. Generate Report\n6. Exit")
    userInput = int(input())
    match userInput:
        case 1:
            print("\nCreating Tables...")
            A2Functions.createTable(dynamo_res, "shortlist_area")
            A2Functions.createTable(dynamo_res, "shortlist_capitals")
            A2Functions.createTable(dynamo_res, "shortlist_languages")
            A2Functions.createTable(dynamo_res, "un_shortlist")
            A2Functions.createTable(dynamo_res, "shortlist_gdppc")
            A2Functions.createTable(dynamo_res, "shortlist_curpop")
            print("\n")
            print("Loading Data")
            A2Functions.bulkLoad(dynamo_res, "shortlist_area")
            A2Functions.bulkLoad(dynamo_res, "shortlist_capitals")
            A2Functions.bulkLoad(dynamo_res, "shortlist_languages")
            A2Functions.bulkLoad(dynamo_res, "un_shortlist")
            A2Functions.bulkLoad(dynamo_res, "shortlist_gdppc")
            A2Functions.bulkLoad(dynamo_res, "shortlist_curpop")
            print("\n")
        case 2:
            print("\nDeleting Tables...")
            A2Functions.deleteTable(dynamo_res, "yjamal_shortlist_area")
            A2Functions.deleteTable(dynamo_res, "yjamal_shortlist_capitals")
            A2Functions.deleteTable(dynamo_res, "yjamal_shortlist_languages")
            A2Functions.deleteTable(dynamo_res, "yjamal_un_shortlist")
            A2Functions.deleteTable(dynamo_res, "yjamal_shortlist_gdppc")
            A2Functions.deleteTable(dynamo_res, "yjamal_shortlist_curpop")
            print("\n")
        case 3:
            tableName = ""
            addRecord = ""
            countryName = ""
            JSONinput = ""
            print("Enter a valid table name to edit:")
            tableName = input()
            print("Enter action 'Add', 'Update' or 'Delete':")
            addRecord = input()
            print("Enter Country Name:")
            countryName = input()
            
            if ("Add" in addRecord):
                print("Enter JSON add command (example for area: {'ISO3' : 'KZN', 'Area' : '26503'})")
                JSONinput = input()
                A2Functions.addRecord(dynamo_res, tableName, countryName, JSONinput)
                print("")

            elif(addRecord == "Update"):
                print("Enter JSON update command (example for area: {'ISO3' : 'KZN', 'Area' : '26503'})")
                JSONinput = input()
                A2Functions.updateRecord(dynamo_res, tableName, countryName, JSONinput)
                
            else:
                print("Deleting Record...")
                A2Functions.deleteRecord(dynamo_res, tableName, countryName)
        case 4:
            tableName = ""
            print("Enter Table name to display data:")
            tableName = input()
            A2Functions.displayData(dynamo_res, tableName)
        case 5:
            reportType = ""
            print("Select your type of report (A/B):")
            reportType = input()
            match reportType:
                case "A":
                    countryName = ""
                    print("Input Country Name")
                    countryName = input()
                    A2Functions.reportA(dynamo_res, countryName)
                case "B":
                    reportYear = ""
                    print("Enter year for the Global report: ")
                    reportYear = input()
                    A2Functions.reportB(dynamo_res, reportYear)
        case 6:
            break

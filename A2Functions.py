import configparser
import os 
import sys 
import pathlib
import boto3
import pathlib
import readline
import traceback
import csv
from collections import OrderedDict
import json
import pandas as pd
import numpy as np
from tabulate import tabulate
from boto3.dynamodb.conditions import Key, Attr

#create tables
def createTable(dynamo_res, csvName):
    try:
        schema = []
        attrDefs = []
        match csvName:
            case "shortlist_area":
                schema = [{'AttributeName': 'Country Name','KeyType': 'HASH'}]
                attrDefs = [{'AttributeName': 'Country Name','AttributeType': 'S'}]
            case "shortlist_languages":
                schema = [{'AttributeName': 'Country Name','KeyType': 'HASH'}]
                attrDefs = [{'AttributeName': 'Country Name','AttributeType': 'S'}]
            case "shortlist_capitals":
                schema = [{'AttributeName': 'Country Name','KeyType': 'HASH'}]
                attrDefs = [{'AttributeName': 'Country Name','AttributeType': 'S'}]
            case "un_shortlist":
                schema = [{'AttributeName': 'Country Name','KeyType': 'HASH'}]
                attrDefs = [{'AttributeName': 'Country Name','AttributeType': 'S'}]
            case "shortlist_curpop":
                schema = [{'AttributeName': 'Country','KeyType': 'HASH'}]
                attrDefs = [{'AttributeName': 'Country','AttributeType': 'S'}]
            case "shortlist_gdppc":
                schema = [{'AttributeName': 'Country','KeyType': 'HASH'}]
                attrDefs = [{'AttributeName': 'Country','AttributeType': 'S'}]
        
        newTable = dynamo_res.create_table(TableName="yjamal_"+csvName, KeySchema=schema, AttributeDefinitions=attrDefs, ProvisionedThroughput={'ReadCapacityUnits': 10,'WriteCapacityUnits': 10}) 
        newTable.meta.client.get_waiter('table_exists').wait(TableName="yjamal_"+csvName)
        print("yjamal_"+csvName+" has been created")   
    except Exception as e:
        print("Cannot create table yjamal_"+csvName)
        print(e)

def deleteTable(dynamo_res, tableName):
    try:
        table = dynamo_res.Table(tableName)
        table.delete()
        print(tableName+" Deleted")
    except:
        print("Couldn't delete "+tableName)


def bulkLoad(dynamo_res, csvName):
    try:
        tableName="yjamal_"+csvName
        table = dynamo_res.Table(tableName)

        #both of these work with vanilla input
        if (csvName == "shortlist_area" or csvName == "shortlist_capitals"):
            with open(csvName+".csv", 'r') as file:
                reader = csv.DictReader(file)
                for row in reader:
                    table.put_item(Item=row)

        
        match tableName:
            #shortlist_languages needs to be edited a bit to account for multiple languages
            case "yjamal_shortlist_languages":
                with open(csvName+".csv", 'r') as file:
                    reader = csv.DictReader(file)
                    for row in reader:
                        if None in row:
                            row['Languages'] = [row['Languages'], row[None][0]]
                            row.pop(None, None)
                        table.put_item(Item=row)

            #un_shortlist needs to have its fieldnames defined
            case "yjamal_un_shortlist":
                with open(csvName+".csv", 'r') as file:
                    reader = csv.DictReader(file, fieldnames=['ISO3','Country Name', 'Full Country Name', 'ISO2'])
                    for row in reader:
                        table.put_item(Item=row)
            #shortlist_gdppc needs to have it's first line changed
            case "yjamal_shortlist_gdppc":
                with open(csvName+".csv", 'r') as file:
                    reader = csv.DictReader(file, fieldnames=['Country'] + [''+str(i)+'' for i in range(1970,2020)])
                    next(file)
                    for row in reader:
                        table.put_item(Item=row)
            #shortlist_curpop needs some tweaking to its fieldnames
            case "yjamal_shortlist_curpop":
                with open(csvName+".csv", 'r') as file:
                    reader = csv.DictReader(file, fieldnames=['Country', 'Currency'] + [''+str(i)+'' for i in range(1970,2020)])
                    next(file)
                    for row in reader:
                        table.put_item(Item=row)
        print("Successfully Loaded "+csvName+".csv to "+tableName)
        
    except Exception as e:
        print("Cannot Load CSV data into "+tableName)
        print(e)

def addRecord(dynamo_res, tableName, country, JSONInput):
    try:
        #get table, parse user inputted JSON and then put_item
        table = dynamo_res.Table(tableName)

        #This is for the long-dated entries
        if (tableName == "yjamal_shortlist_curpop" or tableName == "yjamal_shortlist_gdppc"):
            if (tableName == "yjamal_shortlist_curpop"):
                inputRecord = {'Country' : ''+country+'', 'Currency':eval(JSONInput)['Currency']}
            else:
                inputRecord = {'Country' : ''+country+''}
            inputRecord = {**inputRecord, **{str(i):"<empty>" for i in range(1970,2020)}}
            for key in eval(JSONInput):
                if (key != 'Currency'):
                    inputRecord[key] = eval(JSONInput)[key]
            table.put_item(Item=inputRecord)

        else: #User can enter each JSON manually since these aren't insanely long schema
            inputRecord = {'Country Name' : ''+country+''}
            inputRecord = {**inputRecord, **eval(JSONInput)}       
            table.put_item(Item=inputRecord)
        print("Item "+str(inputRecord)+" added")
        return
    except Exception as e:
        print("Cannot add "+JSONInput+" to "+tableName)
        print(e)

def updateRecord(dynamo_res, tableName, country, JSONInput):
    try:
        table = dynamo_res.Table(tableName)
        if (tableName == "yjamal_shortlist_curpop" or tableName == "yjamal_shortlist_gdppc"):
            searchKey = {'Country' : ''+country+''}
        else:
            searchKey = {'Country Name' : ''+country+''}
        inputRecord = eval(JSONInput)

        for key in inputRecord:
            table.update_item(Key=searchKey, AttributeUpdates={key : {'Action':'PUT', 'Value':inputRecord[key]}})
        print("Item "+str(inputRecord)+" updated into "+tableName)
        return
    except Exception as e:
        print("Cannot update "+JSONInput+" into "+tableName)
        print(e)

def deleteRecord(dynamo_res, tableName, country):
    try:
        table = dynamo_res.Table(tableName)

        if (tableName == "yjamal_shortlist_curpop" or tableName == "yjamal_shortlist_gdppc"):
            table.delete_item(Key={'Country': ''+country+''})
        else:
            table.delete_item(Key={'Country Name': ''+country+''})
        return
    except Exception as e:
        print("cannot delete "+country+" in "+tableName)
        print(e)

def displayData(dynamo_res, tableName):
    try:
        table = dynamo_res.Table(tableName)
        for item in table.scan()['Items']:
            print(item)
        return
    except Exception as e:
        print("cannot display "+tableName)
        print(e)

def queryAttr(table, key, keyVal, attribute):
    try:
        x = table.query(KeyConditionExpression=Key(key).eq(keyVal))['Items'][0][attribute]
        if (x == ""):
            return 0
        return x
    except Exception as e:
        #print("Table doesn't have"+attribute)
        return 0


def rankAttr(table, attribute):
    try:
        attrDict = table.scan(FilterExpression=Attr(attribute).exists())

        attrDict = attrDict['Items']
        countryRankingList = {}
        
        i = 0
        ogLength = len(attrDict)
        while (i < ogLength):
            maxNum = sys.maxsize
            chosenItem = {}
            popIndex = 0
            k = 0
            for item in attrDict:
                itemVal = item[attribute]
                if (itemVal == ""): 
                    itemVal = 0
                curNum = int(itemVal)
                if (curNum < maxNum):
                    chosenItem = item
                    maxNum = curNum
                    popIndex = k
                k += 1
            attrDict.pop(popIndex)
            if (table.name == "yjamal_shortlist_gdppc" or table.name == "yjamal_shortlist_curpop"):
                countryRankingList[chosenItem['Country']] = ogLength - i
            else:   
                countryRankingList[chosenItem['Country Name']] = ogLength - i
            i += 1
        #print(countryRankingList)
        return countryRankingList
    except Exception as e:
        print(traceback.format_exc())

def globalDensityAndRank(popTable, areaTable, year):
    try:
        densityRankingList = {}
        countryDensities = {}
        populationDict = popTable.scan(FilterExpression=Attr(year).exists())['Items']

        for item in populationDict:
            if (item[year] == ''):
                countryDensities[item['Country']] = 0
            else:
                countryDensities[item['Country']] = float(item[year]) / float(queryAttr(areaTable, "Country Name", item['Country'], "Area"))
            
        
        densityRankingList = {k: v for k, v in sorted(countryDensities.items(), key=lambda item: item[1], reverse=True)}
        i = 1
        for country in densityRankingList:
            densityRankingList[country] = i
            i += 1
        return densityRankingList
    except Exception as e:
        print("Cannot obtain density and rank for"+year)

def reportA(dynamo_res, countryName):
    try:
        unTable = dynamo_res.Table("yjamal_un_shortlist")
        areaTable = dynamo_res.Table("yjamal_shortlist_area")
        capitalTable = dynamo_res.Table("yjamal_shortlist_capitals")
        langTable = dynamo_res.Table("yjamal_shortlist_languages")
        popTable = dynamo_res.Table("yjamal_shortlist_curpop")
        gdpTable = dynamo_res.Table("yjamal_shortlist_gdppc")

        print("\n\n"+"\x1B[4m"+countryName+"\x1B[0m")
        print("[Official Name: "+queryAttr(unTable, "Country Name", countryName, "Full Country Name")+"]")
        #get area ranking
        areaRank = rankAttr(areaTable, "Area")
        print(queryAttr(areaTable, "Country Name", countryName, "Area")+" sq km ("+str(areaRank[countryName])+")")
        print("Official Languages: "+str(queryAttr(langTable, "Country Name", countryName, "Languages")))
        print("Capital City: "+queryAttr(capitalTable, "Country Name", countryName, "Capital")+"\n")
        print("\x1B[4m"+"Population"+"\x1B[0m")

        populationScan = popTable.scan(FilterExpression=Key('Country').eq(countryName))['Items'][0]
        populationDict = {'Year':[], 'Population': [], 'Pop Rank': [], 'Population Density (people/sq km)': []}#, 'Density Rank': []}
        
        #fill in first 4 columns of the population
        for item in populationScan:
            if (item == "Currency" or item == "Country"):
                continue
            else:
                populationDict['Year'].append(item)
                
                if (populationScan[item] == ""):
                    populationDict['Population'].append(np.nan)
                    populationDict['Pop Rank'].append("")
                    populationDict['Population Density (people/sq km)'].append("")
                else:
                    populationDict['Population'].append(populationScan[item])
                    populationDict['Pop Rank'].append(rankAttr(popTable, item)[countryName])
                    populationDict['Population Density (people/sq km)'].append(float(populationScan[item])/float(queryAttr(areaTable, "Country Name", countryName, "Area")))

        popData = pd.DataFrame(populationDict)
        popData = popData.sort_values("Year")
        popData.dropna(subset=['Population'], inplace=True)
        
        #Get all population densities
        densityRank = []
        print("Calculating Population...")
        for year in popData['Year']:
            densityRank.append(globalDensityAndRank(popTable, areaTable, year)[countryName])
        popData['Density Rank'] = densityRank
        print(tabulate(popData, headers='keys', tablefmt='simple_grid', showindex="never"))
        
        print("\x1B[4m"+"Economics"+"\x1B[0m")
        print("Currency: "+queryAttr(popTable, "Country", countryName, "Currency"))

        economicsDict = {'Year':popData['Year'] , 'GDPPC':[], 'Rank': []}
        for year in economicsDict['Year']:
            economicsDict['GDPPC'].append(queryAttr(gdpTable, "Country", countryName, year))
            rankedGDP = rankAttr(gdpTable, year)
            if (rankedGDP):
                economicsDict['Rank'].append(rankedGDP[countryName])
            else:
               economicsDict['Rank'].append(0) 

        gdpData = pd.DataFrame(economicsDict)
        gdpData = gdpData.sort_values("Year")
        gdpData.dropna(subset=['GDPPC'], inplace=True)
        print(tabulate(gdpData, headers='keys', tablefmt='simple_grid', showindex="never"))
        print("\n\n")
    except Exception as e:
        print("Couldn't Generate report for "+countryName)
        print(traceback.format_exc())

def reportB(dynamo_res, repYear):
    try:  
        unTable = dynamo_res.Table("yjamal_un_shortlist")
        areaTable = dynamo_res.Table("yjamal_shortlist_area")
        capitalTable = dynamo_res.Table("yjamal_shortlist_capitals")
        langTable = dynamo_res.Table("yjamal_shortlist_languages")
        popTable = dynamo_res.Table("yjamal_shortlist_curpop")
        gdpTable = dynamo_res.Table("yjamal_shortlist_gdppc")

        print("\n\n"+"\x1B[4m"+"Global Report"+"\x1B[0m")
        print("Year: "+repYear)
        numCountries = capitalTable.scan(FilterExpression=Attr("Country Name").exists())['Count']
        print("Number of Countries: "+str(numCountries))
        print("\n"+"\x1B[4m"+"Table of Countries Ranked by Population (Largest to Smallest)"+"\x1B[0m")

        #Obtain ranks and all, then obtain population counts for pop Table
        populationDict = {'Country Name':[], 'Population': [], 'Rank':[]}
        rankAndCountry = rankAttr(popTable, repYear)
        for country in rankAndCountry:
            populationDict['Country Name'].append(country)
            populationDict['Population'].append(queryAttr(popTable, 'Country', country, repYear))
            populationDict['Rank'].append(rankAndCountry[country])
        popData = pd.DataFrame(populationDict)
        popData = popData.sort_values("Rank")
        print(tabulate(popData, headers='keys', tablefmt='simple_grid', showindex="never"))

        #Table of countries ranked by Area
        print("\n"+"\x1B[4m"+"Table of Countries Ranked by Area (Largest to Smallest)"+"\x1B[0m")
        areaDict = {'Country Name':[], 'Area': [], 'Rank':[]}
        rankAndCountry = rankAttr(areaTable, "Area")
        for country in rankAndCountry:
            areaDict['Country Name'].append(country)
            areaDict['Area'].append(queryAttr(areaTable, 'Country Name', country, "Area"))
            areaDict['Rank'].append(rankAndCountry[country])
        areaData = pd.DataFrame(areaDict)
        areaData = areaData.sort_values("Rank")
        print(tabulate(areaData, headers='keys', tablefmt='simple_grid', showindex="never"))

        #Table of Countries ranked by Pop Dens
        print("\n"+"\x1B[4m"+"Table of Countries Ranked by Density (Largest to Smallest)"+"\x1B[0m")
        densDict = {'Country Name':[], 'Density (people / sq km)':[], 'Rank':[]}
        rankedAndCountry = globalDensityAndRank(popTable, areaTable, repYear)
        for country in rankAndCountry:
            if (country in rankedAndCountry):   
                densDict['Country Name'].append(country)
                densDict['Density (people / sq km)'].append(float(queryAttr(popTable, 'Country', country, repYear)) / float(queryAttr(areaTable, 'Country Name', country, "Area")))
                densDict['Rank'].append(rankedAndCountry[country])
        densData = pd.DataFrame(densDict)
        densData = densData.sort_values("Rank")
        print(tabulate(densData, headers='keys', tablefmt='simple_grid', showindex="never"))

        #GDP Per Capita for all Countries
        print("\n"+"\x1B[4m"+"GDP Per Capita for all Countries"+"\x1B[0m")
        gdpQuery = gdpTable.scan()['Items']
        gdpData = pd.DataFrame.from_records(gdpQuery)
        gdpData = gdpData.set_index("Country")
        gdpData = gdpData.reindex(sorted(gdpData.columns), axis=1)

        i = 0
        for year in gdpData.columns:
            if (year == repYear):
                break
            i += 1
        gdpData = gdpData.iloc[:, :i+1]
        i = 0
        ogLength = gdpData.columns
        while i < len(ogLength):
            left = gdpData.iloc[:, :10]
            gdpData = gdpData.iloc[:, 10:]
            print(tabulate(left, headers='keys', tablefmt='simple_grid', showindex="always"))
            i += 10
            print("\n")
        #print(tabulate(gdpData, headers='keys', tablefmt='simple_grid', showindex="always"))
        #print(len(gdpData.columns))

    except Exception as e:
        print("Cannot Generate report B for "+repYear)
        print(traceback.format_exc())


    





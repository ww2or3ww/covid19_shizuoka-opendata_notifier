import sys
sys.dont_write_bytecode = True
import os
import os.path
import io
import json
import re
from datetime import datetime
import requests
from retry import retry
import slackweb
from requests_aws4auth import AWS4Auth
import boto3
from boto3.dynamodb.conditions import Key
import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logging.basicConfig(level=logging.INFO)

API_ADDRESS_CSV2JSON        = ""    if("API_ADDRESS_CSV2JSON" not in os.environ)    else os.environ["API_ADDRESS_CSV2JSON"]
API_KEY_CSV2JSON            = ""    if("API_KEY_CSV2JSON" not in os.environ)        else os.environ["API_KEY_CSV2JSON"]
AWS_REGION                  = ""    if("AWS_REGION" not in os.environ)              else os.environ["AWS_REGION"]
AWS_ACCESS_KEY_ID           = ""    if("AWS_ACCESS_KEY_ID" not in os.environ)       else os.environ["AWS_ACCESS_KEY_ID"]
AWS_SECRET_ACCESS_KEY       = ""    if("AWS_SECRET_ACCESS_KEY" not in os.environ)   else os.environ["AWS_SECRET_ACCESS_KEY"]
AWS_SESSION_TOKEN           = ""    if("AWS_SESSION_TOKEN" not in os.environ)       else os.environ["AWS_SESSION_TOKEN"]
AUTH                        = None  if(not AWS_REGION)                              else AWS4Auth(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, 'execute-api', session_token=AWS_SESSION_TOKEN)
SLACK_WEBHOOK_ALERT         = ""    if("SLACK_WEBHOOK_ALERT" not in os.environ)     else os.environ["SLACK_WEBHOOK_ALERT"]
SLACK_WEBHOOK_HAMAMATSU     = ""    if("SLACK_WEBHOOK_HAMAMATSU" not in os.environ) else os.environ["SLACK_WEBHOOK_HAMAMATSU"]
DYNAMODB_NAME               = ""    if("DYNAMODB_NAME" not in os.environ)           else os.environ["DYNAMODB_NAME"]
DYNAMODB_HISTORY_NAME       = ""    if("DYNAMODB_HISTORY_NAME" not in os.environ)   else os.environ["DYNAMODB_HISTORY_NAME"]
S3_BUCKET_NAME              = ""    if("S3_BUCKET_NAME" not in os.environ)          else os.environ["S3_BUCKET_NAME"]

DYNAMO_TABLE                = boto3.resource("dynamodb").Table(DYNAMODB_NAME)
DYNAMO_HISTORY_TABLE        = boto3.resource("dynamodb").Table(DYNAMODB_HISTORY_NAME)
S3                          = boto3.resource('s3') 

class CityInfo:
    def __init__(self, city, queryParam, slackWebHook):
        self.city = city
        self.queryParam = queryParam
        self.slackWebHook = slackWebHook
        
CITIES = [
    CityInfo(
        "hamamatsu", 
        "main_summary:5ab47071-3651-457c-ae2b-bfb8fdbe1af1,patients:5ab47071-3651-457c-ae2b-bfb8fdbe1af1,patients_summary:5ab47071-3651-457c-ae2b-bfb8fdbe1af1,inspection_persons:d4827176-d887-412a-9344-f84f161786a2,contacts:1b57f2c0-081e-4664-ba28-9cce56d0b314", 
        SLACK_WEBHOOK_HAMAMATSU
    ), 
    CityInfo(
        "shizuoka-shi", 
#        "main_summary:c04e2d2f-2ce4-4e32-856a-b7e760ba982d,patients:c04e2d2f-2ce4-4e32-856a-b7e760ba982d,patients_summary:c04e2d2f-2ce4-4e32-856a-b7e760ba982d,inspection_persons:6b102a25-9746-4dac-b6a9-8370afe6af75,contacts:4e25348c-b24d-4bc5-b85b-dac9e2fd2439", 
        "contacts:4e25348c-b24d-4bc5-b85b-dac9e2fd2439", 
        SLACK_WEBHOOK_HAMAMATSU
    )
]

TYPE_NAME = {
    "main_summary"          : "検査陽性者の状況", 
    "patients"              : "陽性患者の属性", 
    "patients_summary"      : "陽性患者数", 
    "inspection_persons"    : "検査実施人数", 
    "contacts"              : "新型コロナウイルス感染症に関する相談件数"
}

def lambda_handler(event, context):
    try:
        for cityInfo in CITIES:
            processNotifier(cityInfo)

    except Exception as e:
        logger.exception(e)
        
def processNotifier(cityInfo):
    try:
        retJson = getJsonFromAPI(cityInfo.city, cityInfo.queryParam)
        
        notifyText = ""
        listTypeID = cityInfo.queryParam.split(",")
        for typeId in listTypeID:
            notifyText += processType(cityInfo, retJson, typeId)

        if notifyText:
            notifyText = "【{0}】\n{1}".format(cityInfo.city, notifyText)
            notifyToSlack(cityInfo.slackWebHook, notifyText)

    except Exception as e:
        logger.exception(e)
        notifyToSlack(SLACK_WEBHOOK_ALERT, "【{0}】raise exception.".format(cityInfo.city))

def notifyToSlack(url, text):
    slack = slackweb.Slack(url=url)
    slack.notify(text=text)

def getJsonFromAPI(city, queryParam):
    logger.info("---{0}-{1}".format(city, queryParam))
    apiResponse = getJsonFromAPIWithRetry(queryParam)
    try:
        di = json.loads(apiResponse.text)
    except Exception as e:
        logger.error(apiResponse.text)
        logger.error(e)
        raise e

    if("hasError" in di):
        if(di["hasError"]):
            logger.error("【{0}】has error.".format(city))
            raise Exception(apiResponse.text)
    else:
        logger.error("【{0}】unknown error.\n{1}".format(city, di))
        raise Exception(apiResponse.text)
        
    return di

@retry(tries=3, delay=1)
def getJsonFromAPIWithRetry(queryParam):
    return requests.get("{0}?type={1}".format(API_ADDRESS_CSV2JSON, queryParam), auth=AUTH, headers={"x-api-key": API_KEY_CSV2JSON})

def splitTypeID(typeId):
    listItem = typeId.split(":")
    return listItem[0], listItem[1]
    
def processType(cityInfo, retJson, typeId):
    notifyText = ""
    
    type, id = splitTypeID(typeId)
    date = retJson[type]["date"]
    logger.info("-- {0} : {1} : {2} --".format(type, id, date))
    
    record = selectItem(cityInfo.city, type)

    if(record["Count"] is 0):
        notifyText = "{0} :\n  {1}\n".format(TYPE_NAME[type], date)
        logger.info(notifyText)
        path = uploadFile(cityInfo.city, type, id, date, retJson[type])
        insertItem(cityInfo.city, type, id, date, TYPE_NAME[type], path)
        insertItemHistory(cityInfo.city, type, date, path)

    elif record["Items"][0]["update"] != date:
        notifyText = "{0} :\n  {1} -> {2}\n".format(record["Items"][0]["name"], record["Items"][0]["update"], date)
        logger.info(notifyText)
        path = uploadFile(cityInfo.city, type, id, date, retJson[type])
        updateItem(cityInfo.city, type, id, date, path)
        insertItemHistory(cityInfo.city, type, date, path)
        
    else:
        logger.info("not diff {0} : {1}".format(record["Items"][0]["name"], date))
    
    return notifyText

@retry(tries=3, delay=1)
def uploadFile(city, type, id, date, jsonData):
    dt = datetime.strptime(date, '%Y/%m/%d %H:%M')
    path = "data/{0}/{1}/{2}/{3}/{4}/{5}-{6}".format(city, type, dt.year, str(dt.month).zfill(2), str(dt.day).zfill(2), str(dt.hour).zfill(2), str(dt.minute).zfill(2))
    
    objJson = S3.Object(S3_BUCKET_NAME, "{0}.json".format(path))
    objJson.put(Body = json.dumps(jsonData, ensure_ascii=False, indent=2))

    csvData = getCSVData(id)    
    objCSV = S3.Object(S3_BUCKET_NAME, "{0}.csv".format(path))
    objCSV.put(Body = csvData)

    return path
    
@retry(tries=3, delay=1)
def getCSVData(id):
    apiAddress = "https://opendata.pref.shizuoka.jp/api/package_show?id=" + id
    apiResponse = requests.get(apiAddress).json()
    resources = apiResponse["result"]["resources"]
    
    apiResources = None
    csvAddress = None
    for i in range(len(resources)):
        apiResources = resources[i]
        csvAddress = apiResources["download_url"]
        root, ext = os.path.splitext(csvAddress)
        if ext.lower() == ".csv":
            logger.info(csvAddress)
            break
        
    return requests.get(csvAddress).content

@retry(tries=3, delay=1)
def insertItem(city, type, id, date, name, path):
    DYNAMO_TABLE.put_item(
      Item = {
        "city": city, 
        "type": type, 
        "id": id, 
        "update" : date, 
        "name" : name, 
        "path" : path
      }
    )
    
@retry(tries=3, delay=1)
def insertItemHistory(city, type, date, path):
    DYNAMO_HISTORY_TABLE.put_item(
      Item = {
        "city:type": "{0}:{1}".format(city, type), 
        "datetime": date, 
        "path" : path
      }
    )
    
@retry(tries=3, delay=1)
def updateItem(city, type, id, date, path):
    DYNAMO_TABLE.update_item(
        Key={
            "city": city,
            "type": type,
        },
        UpdateExpression="set #update = :update, #path = :path",
        ExpressionAttributeNames={
            "#update": "update", 
            "#path": "path"
        },
        ExpressionAttributeValues={
            ":update": date, 
            ":path": path
        }
    )

@retry(tries=3, delay=1)
def selectItem(city, type):
    return DYNAMO_TABLE.query(
        KeyConditionExpression=Key("city").eq(city) & Key("type").eq(type)
    )

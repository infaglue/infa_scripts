import json
import requests
import sys
import getopt
from csv import writer
from pathlib import Path
from setup import *
import logging

# ---------------------------------------------------------------------------------------------------------------------------------------------
# Overview -
#
# This script will take an asset that is provided and export inbound and outbound lineage, like how EDC use too.
# ---------------------------------------------------------------------------------------------------------------------------------------------

loglevel = 1
processedAssets = []
mainAssetInfo = []
sessionID = []
idmcUsers = []

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)-5s - %(message)s'
)

# ----------------------------------------------------------------------------------------------------------------------------------------------
# Functions
# ----------------------------------------------------------------------------------------------------------------------------------------------

def idmc_login(username, password, url):

    logging.info("Login to IDMC server")
    
    url = url + "/identity-service/api/v1/Login"
    raw_data = '{"username": "' + username + '","password": "' + password + '"}'
    headers = {'Content-type': 'application/json'}

    response = requests.post(url, data=raw_data, headers=headers)
    loginInfo = response.text
    loginInfo = json.loads(loginInfo)

    if response.status_code == 400:
        logging.info("Error Logging in")
        print(response.text)
        exit(1)

    return loginInfo


def generate_token(sessionID, url):

    logging.info("Generate Bearer Token")
    url = url + "/identity-service/api/v1/jwt/Token?client_id=cdlg_app&nonce=1234"
    headers = {'Content-type': 'application/json', 'cookie': 'USER_SESSION=' + sessionID, 'IDS-SESSION-ID': sessionID}

    response = requests.post(url, headers=headers)
    tokenJson = response.text
    tokenJson = json.loads(tokenJson)

    return tokenJson


def get_idmc_users(url, bearToken):

    global sessionID

    logging.info("Getting Platform Users")
    url = url + "/saas/public/core/v3/users"

    headers = {'Content-type': 'application/json', 'Authorization': 'Bearer ' + bearToken, 'INFA-SESSION-ID': sessionID}
    response = requests.get(url, headers=headers)
    resultJson = response.text

    return json.loads(resultJson)


def get_asset(url, orgID, bearToken):
    
    logging.debug("Getting Asset")

    headers = {'Content-type': 'application/json', 'X-INFA-ORG-ID': orgID, 'Authorization': 'Bearer ' + bearToken}
    response = requests.get(url, headers=headers)
    assetInfo = response.text

    if response.status_code != 200:
        logging.debug("    - API Response code = " + str(response.status_code))
        logging.debug("    - API call = " + url)
        return
    else:
        assetInfo = json.loads(assetInfo)

    return assetInfo


def process_lineage(assetID, loginInfo, bearToken, direction, writeFileFlag):
    global processedAssets
    stakeholderList = []

    url = cdgc_api_url + "/data360/search/v1/assets/" + assetID + "?scheme=internal&segments=all,lineage-direction:" + direction
    assetInfo = get_asset(url, loginInfo['orgId'], bearToken)

    if assetInfo is not None:
        logging.info("    - Asset Name : " + assetInfo['summary']['core.name'])
        logging.info("    - Asset Type : " + assetInfo['systemAttributes']['core.classType'])
        logging.info("    - Resource name : " + assetInfo['selfAttributes']['core.resourceName'])
        logging.info("    - Resource type : " + assetInfo['selfAttributes']['core.resourceType'])
        #json_formatted_str = json.dumps(assetInfo, indent=2)
        #print(json_formatted_str)

    #if assetInfo is not None and 'stakeholdership' in assetInfo:
    stakeholderList = []

    if writeFileFlag == "Y":
        write_output(assetInfo, direction, stakeholderList)

    # Process Lineage for this object
    if assetInfo is not None and 'lineage' in assetInfo:
        for lineage in assetInfo['lineage']:
            for hops in lineage['hops']:
                logging.info("    - Lineage found!")
                for lineageItems in hops['items']:
                    if direction == "inbound":
                        lineageField = "fromUri"
                        lineageTitle = "from"
                        lineageType = "fromType"
                    else:
                        lineageField = "toUri"
                        lineageTitle = "to"
                        lineageType = "toType"

                    relatedAssetID = lineageItems['details'][lineageField].split("/")[5]
                    relatedAssetID = relatedAssetID.split("?")[0]

                    if relatedAssetID in processedAssets:
                        logging.info("    - Loop found, skipping")
                    else:
                        processedAssets.append(relatedAssetID)
                        logging.info("    - Found Lineage To : " + lineageItems[lineageTitle] + " (" + lineageItems[lineageType] + ")")
                        logging.info("    - -----------------")
                        process_lineage(relatedAssetID, loginInfo, bearToken, direction, "Y")


def write_output(assetInfo, direction, stakeholderList):

    global mainAssetInfo

    if not stakeholderList:
        stakeholderList = ""

    fileName = mainAssetInfo['summary']['core.name'] + "_" + direction + ".csv"
    fileCheck = Path(fileName)

    logging.info("    - Writing to File")

    if not fileCheck.is_file():
        with open(fileName, 'a', newline='') as fileCSV:
            csvAppend = writer(fileCSV)
            csvAppend.writerow(["Name", "Asset ID", "Class Type", "Resource Name", "Resource Type", "Stakeholders", "Asset URL"])

    with open(fileName, 'a', newline='') as fileCSV:
        csvAppend = writer(fileCSV)
        csvAppend.writerow([assetInfo['summary']['core.name'], assetInfo['core.identity'], assetInfo['systemAttributes']['core.classType'], assetInfo['selfAttributes']['core.resourceName'], assetInfo['selfAttributes']['core.resourceType'], stakeholderList, assetInfo['core.identity']])

    fileCSV.close()

# ----------------------------------------------------------------------------------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------------------------------------------------------------------------------
def main(argv):
    # Set Parameters
    global mainAssetInfo
    global sessionID
    global idmcUsers

    assetID = ""

    arg_help = f"""cdgc_export_lineage.py -a <asset_id>
        -h              help
        -a <asset id>   ID of the asset 
    """.format(argv[0])

    # Fetch and Test Command Line Arguments
    try:
        opts, args = getopt.getopt(argv[1:], "ha:", ["help", "asset="])
        arg1 = argv[1]
    except IndexError:
        print(arg_help)
        sys.exit(2)

    for opt, arg in opts:
        if opt in ("-h", "--help"):
            print(arg_help)  # print the help message
            sys.exit(2)
        elif opt in ("-a", "--asset"):
            assetID = arg

    logging.info("Starting")
    logging.info("Parameters")
    logging.info("    - Starting Asset ID: " + assetID)

    # Login and set variables
    loginInfo = idmc_login(username, password, login_url)
    sessionID = loginInfo['sessionId']
    tokenJson = generate_token(loginInfo['sessionId'], login_url)
    bearToken = tokenJson['jwt_token']

    url = cdgc_api_url + "/data360/search/v1/assets/" + assetID + "?scheme=internal&segments=all,lineage-direction:inbound"
    mainAssetInfo = get_asset(url, loginInfo['orgId'], bearToken)
    logging.info("    - Starting Asset Name: " + mainAssetInfo['summary']['core.name'])

    idmcUsers = get_idmc_users("https://usw5.dm-us.informaticacloud.com", bearToken)

    # Getting Inbound Lineage
    logging.info("Getting Inbound Lineage Path")
    processedAssets.clear()
    processedAssets.append(assetID)
    process_lineage(assetID, loginInfo, bearToken, "inbound", "N")
    logging.info("No More Inbound Lineage")

    # Getting Outbound Lineage
    logging.info("Getting Outbound Lineage Path")
    process_lineage(assetID, loginInfo, bearToken, "outbound", "N")
    logging.info("No More Outbound Lineage")

    logging.info("Script Finished")


if __name__ == "__main__":
    main(sys.argv)

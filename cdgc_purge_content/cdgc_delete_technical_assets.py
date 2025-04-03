import json
import datetime
import requests
import sys
import time
from random import randrange
import getopt
from multiprocessing.pool import ThreadPool
from setup import *
import logging

# ---------------------------------------------------------------------------------------------------------------------------------------------
# Overview -
#
# This script will purge metadata from a specific scanner, or all scanners. It also can delete the scanner once the purge is finished.
# ---------------------------------------------------------------------------------------------------------------------------------------------

concurrentThreads = 8
statusTimeout = 45
deleteScannerFlag = "N"
scannerToPurge = "All"
allScannersFlag = "N"
loglevel = 1

orgID = sessionID = token = ""

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)-5s - %(message)s'
)

# ---------------------------------------------------------------------------------------------------------------------------------------------
# Functions
# ---------------------------------------------------------------------------------------------------------------------------------------------
def idmc_login(idmcUsername, idmcPassword, url):

    url = url + "/identity-service/api/v1/Login"

    raw_data = {"username": idmcUsername, "password": idmcPassword}
    headers = {"Content-type": "application/json"}

    raw_data = json.dumps(raw_data)

    response = requests.post(url, data=raw_data, headers=headers)
    loginInfo = response.text
    loginInfo = json.loads(loginInfo)

    if "error" in loginInfo:
        logging.error(f"Error logging into IDMC : {loginInfo['error']['message']}")
        exit(1)

    return loginInfo


def generate_token(url):

    url = url + "/identity-service/api/v1/jwt/Token?client_id=cdlg_app&nonce=1234"
    headers = {
        "Content-type": "application/json",
        "cookie": "USER_SESSION=" + sessionID,
        "IDS-SESSION-ID": sessionID
    }

    response = requests.post(url, headers=headers)
    tokenJson = response.text
    tokenJson = json.loads(tokenJson)

    if "error" in tokenJson:
        logging.error(f"Error getting token: {tokenJson['error']['message']}")
        logging.error(f"Session ID = {sessionID}")
        exit(1)

    return tokenJson


def get_catalog_sources(url):
    
    url = url + "/ccgf-catalog-source-management/api/v1/datasources?offset=0&limit=25&sort=name:ASC"
    headers = {'Content-type': 'application/json', 'X-Infa-Org-Id': orgID, 'Authorization': 'Bearer ' + token}

    response = requests.get(url, headers=headers)
    catalogSource = response.text
    catalogSource = json.loads(catalogSource)

    return catalogSource


def purge_catalog_source(url, scannerName):
    url = url + "/ccgf-catalog-source-management/api/v1/datasources/" + scannerName + "?type=purge"
    headers = {'Content-type': 'application/json', 'X-Infa-Org-Id': orgID, 'Authorization': 'Bearer ' + token, 'IDS-SESSION-ID': sessionID}

    response = requests.delete(url, headers=headers)
    purgeCatalog = response.text
    purgeCatalog = json.loads(purgeCatalog)

    return purgeCatalog


def delete_catalog_source(url, scannerName):
    url = url + "/ccgf-catalog-source-management/api/v1/datasources/" + scannerName + "?type=soft"
    headers = {'Content-type': 'application/json', 'X-Infa-Org-Id': orgID, 'Authorization': 'Bearer ' + token, 'IDS-SESSION-ID': sessionID}

    response = requests.delete(url, headers=headers)
    purgeCatalog = response.text
    purgeCatalog = json.loads(purgeCatalog)

    return purgeCatalog


def get_job_info(url, scannerID):
    url = url + "/ccgf-orchestration-management-api-server/api/v1/jobs/" + scannerID + "?aggregateResourceUsage=false&expandChildren=INPUT-PROPERTIES&expandChildren=OUTPUT-PROPERTIES&expandChildren=TASK-HIERARCHY&expandChildren=WORKFLOW-DETAILS&expandChildren=OPERATIONS"
    headers = {'Content-type': 'application/json', 'X-Infa-Org-Id': orgID, 'Authorization': 'Bearer ' + token, 'IDS-SESSION-ID': sessionID}

    response = requests.get(url, headers=headers)
    jobInfoJson = response.text
    jobInfoJson = json.loads(jobInfoJson)

    return jobInfoJson


def check_scanner_status(url, jobId, name):

    global statusTimeout
    global deleteScannerFlag

    keepCheckingFlag = "Y"
    statusList = ['COMPLETED', 'FAILED', 'COMPLETED WITH ERRORS', 'PARTIAL_COMPLETED']

    while keepCheckingFlag == "Y":
        jobInfo = get_job_info(url, jobId)
        logging.info("Checking Job Status [" + name + "] : " + jobInfo['status'])

        # Check if the purge has completed (even with errors)
        if jobInfo['status'] in statusList:
            keepCheckingFlag = "N"

            # Delete scanner if the user wants to delete
            if deleteScannerFlag == "Y" and jobInfo['status'] == "COMPLETED":
                logging.info("Deleting Scanner: " + name)
                delete_catalog_source(url, name)
        else:
            time.sleep(statusTimeout)


def process_scanner(scanner, singleScanner=""):

    logging.info("Requesting Scanner to Purge : " + scanner['name'])

    # pause for a random amount, so we're not firing every API request off at once
    time.sleep(randrange(5))

    jobResponse = purge_catalog_source(cdgc_api_url, scanner['name'])
    if "jobId" in jobResponse:
        check_scanner_status(cdgc_api_url, jobResponse['jobId'], scanner['name'])

# ---------------------------------------------------------------------------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------------------------------------------------------------------------
def main(argv):

    # Set Parameters
    arg_help = f"""cdgc_delete_technical_assets.py -h -s <scanner> -a -d -u <username> -p <password>
        -h              help
        -s  <scanner>   Purge Specific Scanner
        -a              Purge All Scanners
        -u  <username>  Username to log into IDMC
        -p  <password>  Password to log into IDMC
        -d              Delete Scanner after it's purged
    """.format(argv[0])

    global deleteScannerFlag, allScannersFlag, scannerToPurge, orgID, sessionID, token, username, password

    # Fetch and Test Command Line Arguments
    try:
        opts, args = getopt.getopt(argv[1:], "has:du:p:", ["help", "all", "scanner=", "delete", "username=", "password="])
    except:
        print(arg_help)
        sys.exit(2)
    for opt, arg in opts:
        if opt in ("-h", "--help"):
            print(arg_help)  # print the help message
            sys.exit(2)
        elif opt in ("-d", "--delete"):
            deleteScannerFlag = "Y"
        elif opt in ("-a", "--all"):
            allScannersFlag = "Y"
        elif opt in ("-s", "--scanner="):
            scannerToPurge = arg
        elif opt in ("-u", "--username"):
            username = arg
        elif opt in ("-p", "--password"):
            password = arg

    if allScannersFlag == "N" and scannerToPurge == "All":
        print("ERROR: You must include a scanner to purge (-s <scanner>) or purge all scanners (-a)\n")
        print(arg_help)
        sys.exit(2)

    logging.info(f"Starting Script")
    logging.info(f"Parameter -> User: {username}")
    logging.info(f"Parameter -> Scanner to Purge: {scannerToPurge}")
    logging.info(f"Parameter -> Delete Scanner after Purge: {deleteScannerFlag}")
    logging.info(f"Parameter -> Concurrent Processes: {concurrentThreads}")

    # Login and set variables
    logging.info(f'Logging into IDMC')
    loginInfo = idmc_login(username, password, login_url)
    orgID = loginInfo['orgId']
    sessionID = loginInfo['sessionId']

    logging.info(f'Generate Access Token')
    tokenJson = generate_token(login_url)
    token = tokenJson['jwt_token']

    # Get entire list of scanners
    logging.info("Getting scanner list from MCC")
    catalogSources = get_catalog_sources(cdgc_api_url)

    for index, item in enumerate(catalogSources['datasources']):
        item["token"] = token
        item["loginInfo"] = loginInfo
        catalogSources['datasources'][index] = item

    if allScannersFlag == "Y":
        if catalogSources:
            pool = ThreadPool()
            with ThreadPool(concurrentThreads) as pool:
                pool.map(process_scanner, catalogSources['datasources'])
            pool.close()

    if allScannersFlag == "N":
        if catalogSources:
            for scanner in catalogSources['datasources']:
                if scanner['name'] != scannerToPurge:
                    continue
                process_scanner(scanner)

    logging.info("Script Completed")


if __name__ == "__main__":

    if ok_to_delete != "Y":
        print(f"Please update setup.py and set ok_to_delete to confirm it's ok to delete assets")
        sys.exit(2)

    main(sys.argv)

import json
import datetime
import requests
import sys
import getopt
import logging

from requests_toolbelt.multipart.encoder import total_len

from setup import *

# ---------------------------------------------------------------------------------------------------------------------------------------------
# Overview -
#
# This script will find assets with lineage
# ---------------------------------------------------------------------------------------------------------------------------------------------

loglevel = 1
bulkAssetLimit = 5
searchAssetCount = 50  # Max 100 due to API limitations, recommend set to a factor of 5
apiTimeout = 120

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)-5s - %(message)s'
)

# ----------------------------------------------------------------------------------------------------------------------------------------------
# Functions
# ----------------------------------------------------------------------------------------------------------------------------------------------

def idmc_login(username, password, url):

    logging.info("Logging into IDMC")

    url = url + "/identity-service/api/v1/Login"
    raw_data = '{"username": "' + username + '","password": "' + password + '"}'
    headers = {'Content-type': 'application/json'}

    response = requests.post(url, data=raw_data, headers=headers)
    loginInfo = response.text
    loginInfo = json.loads(loginInfo)

    if response.status_code == 400:
        logging.error("Error Logging in")
        print(response.text)
        exit(1)

    return loginInfo


def generate_token(sessionID, url):

    logging.info("Generating Bearer Token")
    url = url + "/identity-service/api/v1/jwt/Token?client_id=cdlg_app&nonce=1234"
    headers = {'Content-type': 'application/json', 'cookie': 'USER_SESSION=' + sessionID, 'IDS-SESSION-ID': sessionID}

    response = requests.post(url, headers=headers)
    tokenJson = response.text
    tokenJson = json.loads(tokenJson)

    return tokenJson


def search_cdgc(url, orgID, bearToken, searchTerm, segments, startpos=0):

    global searchAssetCount

    logging.info("Searching for : " + searchTerm + " (row start = " + str(startpos) + ")")
    url = url + "/data360/search/v1/assets?knowledgeQuery=" + searchTerm + "&segments=" + segments

    raw_data = '{"from": ' + str(startpos) + ',"size": ' + str(searchAssetCount) + '}'
    headers = {'Content-type': 'application/json', 'X-INFA-ORG-ID': orgID, 'Authorization': 'Bearer ' + bearToken}

    response = requests.post(url, headers=headers, data=raw_data)
    searchResults = response.text
    searchResults = json.loads(searchResults)

    logging.debug("    - API Response code = " + str(response.status_code))

    return searchResults


def get_asset_bulk(url, orgID, bearToken, assets):

    global apiTimeout

    logging.debug("Getting Assets from API")

    raw_data = assets
    headers = {'Content-type': 'application/json', 'X-INFA-ORG-ID': orgID, 'Authorization': 'Bearer ' + bearToken}

    try:
        response = requests.post(url, headers=headers, data=raw_data, timeout=apiTimeout)
        assetInfo = response.text
        logging.debug("    - API Response code = " + str(response.status_code))

        if response.status_code != 200:
            logging.error("Error getting assets. Unexpected response code")
            return
        else:
            assetInfo = json.loads(assetInfo)

        return assetInfo

    except requests.exceptions.Timeout:
        logging.error("API Call Timed Out! Skipping!")
        return ""


# ----------------------------------------------------------------------------------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------------------------------------------------------------------------------
def main(argv):

    global searchAssetCount

    searchTerm =  resourceName =  resourceType = ""
    lineageHops = lineageAssets = totalAssets = assetsLeft = matchCount = maxDistance = 0
    supressAssetsFlag = "N"


    arg_help = f"""cdgc_list_object_lineage.py -s <term> -n <resource name> -t <resource type> -l <lineage hops> -a <lineage asset count>
        -h              help
        -s  <term>      Search for assets by name (required)
        -r  <name>      Restrict search to a specific resource scanner (optional)
        -t  <type>      Restrict search to a specific resource type (case sensitive!!) (optional)
        -l  <levels>    Number of levels/hops to search for. 2 to 5. APIs do not return anything more than 5 levels (optional) 
        -a  <count>     Asset Count - Lineage must contain at least this many assets (optional)
        -x              Supress Output for Assets that have no lineage (optional)
    """.format(argv[0])

    # Fetch and Test Command Line Arguments
    try:
        opts, args = getopt.getopt(argv[1:], "hs:r:t:l:a:x", ["help", "search=", "resource_name=", "resource_type=", "levels=", "assets=", "supress"])
        arg1 = argv[1]
    except IndexError:
        print(arg_help)
        sys.exit(2)

    for opt, arg in opts:
        if opt in ("-h", "--help"):
            print(arg_help)  # print the help message
            sys.exit(2)
        elif opt in ("-s", "--search"):
            searchTerm = arg
        elif opt in ("-r", "--resource_name"):
            resourceName = arg
        elif opt in ("-t", "--resource_type"):
            resourceType = arg
        elif opt in ("-l", "--levels"):
            lineageHops = arg
        elif opt in ("-a", "--assets"):
            lineageAssets = arg
        elif opt in ("-x", "--supress"):
            supressAssetsFlag = "Y"

    logging.info("Starting Script")
    logging.info("Search Parameters - Search Term: " + searchTerm)

    if resourceName:
        logging.info("Search Parameters - Resource Scanner: " + resourceName)

    if resourceType:
        logging.info("Search Parameters - Resource Type: " + resourceType)

    if int(lineageHops) > 0:
        logging.info("Search Parameters - Lineage Hops: " + str(lineageHops))

    if int(lineageAssets) > 0:
        logging.info("Search Parameters - Lineage Assets: " + str(lineageAssets))

    # Login and set variables
    loginInfo = idmc_login(username, password, login_url)
    tokenJson = generate_token(loginInfo['sessionId'], login_url)
    bearToken = tokenJson['jwt_token']

    # Do a search -- this gets our # of assets we need to start and the first X items, based on searchAssetCount variable
    finalSearchTerm = "(technical dataset *" + searchTerm + "*) "
    if resourceName:
        finalSearchTerm = finalSearchTerm + " in resource \"" + resourceName + "\""

    if resourceType:
        finalSearchTerm = finalSearchTerm + "in catalog source with resource type \"" + resourceType + "\""

    logging.info("Searching for Assets")
    logging.info("Search Syntax : " + finalSearchTerm)
    searchResults = search_cdgc(cdgc_api_url, loginInfo['orgId'], tokenJson['jwt_token'], finalSearchTerm, "summary")

    if searchResults:
        totalAssets = searchResults['summary']['total_hits']
        assetsLeft = int(totalAssets)
        logging.info("Found " + str(totalAssets) + " assets")
    else:
        logging.info("Search Term did not find any results")

    if searchResults:
        # Loop through search results
        for i in range(0, int(totalAssets), searchAssetCount):

            if assetsLeft >= searchAssetCount:
                assetsLeft = int(assetsLeft) - searchAssetCount
                logging.info("Checking next " + str(searchAssetCount) + " Assets (" + str(assetsLeft) + " left)")
            else:
                logging.info("Checking remaining assets")

            searchResults = search_cdgc(cdgc_api_url, loginInfo['orgId'], tokenJson['jwt_token'], finalSearchTerm, "summary", i)

            bulkCount = 0
            assetJson = []
            for asset in searchResults['hits']:

                # Loop through assets, 5 at a time and bulk them for searching
                bulkCount = bulkCount + 1
                if bulkCount <= bulkAssetLimit:
                    assetJson.append(asset['core.identity'])

                # If we reached our limit, start the search process
                if bulkCount == bulkAssetLimit:
                    url = cdgc_api_url + "/data360/search/v1/assets/details?scheme=internal&segments=selfAttributes,summary,lineage-level,lineage-distance:5"
                    assetResults = get_asset_bulk(url, loginInfo['orgId'], bearToken, json.dumps(assetJson, indent=2))

                    try:
                        for bulk_asset in assetResults:
                            logging.debug("Bulk Asset Loop")

                            lineageCount = 0
                            msgs = []
                            if bulk_asset is not None and 'lineage' in bulk_asset:
                                for lineage in bulk_asset['lineage']:
                                    maxDistance = 1
                                    for hops in lineage['hops']:
                                        lineageCount = lineageCount + len(hops['items'])
                                        if hops['distance'] > maxDistance:
                                            maxDistance = hops['distance']

                            if lineageCount >= 1 and int(maxDistance) >= int(lineageHops) and int(lineageCount) >= int(lineageAssets):
                                matchCount = matchCount + 1
                                logging.info(f"Asset : {bulk_asset['summary']['core.name']} (Resource : {bulk_asset['selfAttributes']['core.resourceName']})")
                                logging.info(f"    - This asset has lineage!")

                                logging.info("    - ID: " + bulk_asset['core.identity'])
                                logging.info("    - Lineage Links: " + str(lineageCount))
                                logging.info("    - Hops: " + str(maxDistance))

                    except (Exception,):
                        logging.error("##### ERROR With Parsing Asset Results!", 1)

                    # reset our controls before the next loop
                    bulkCount = 0
                    assetJson = []

            # need to add handler for when there is less than 5 assets
            if bulkCount < 5 & bulkCount > 0:
                logging.info("Less than 5 assets are left, these are skipped for now")

    logging.info("Script Completed")

if __name__ == "__main__":
    main(sys.argv)

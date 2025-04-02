import json
import requests
import sys
import getopt
from multiprocessing.pool import ThreadPool
from setup import *
import logging

# ---------------------------------------------------------------------------------------------------------------------------------------------
# Overview -
#
# This script will attempt to purge all Governance assets. Options are to include how old the assets must be
# This script is used in workshops and testing environments to clean slate an environment or roll it back X days.
# ---------------------------------------------------------------------------------------------------------------------------------------------

loglevel = 1
concurrentThreads = 25
orgID = sessionID = token = ""

delete_count = 1

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)-5s - %(message)s'
)

######################################################################################################
# Functions
######################################################################################################
def idmc_login(idmcUsername, idmcPassword, url):

    url = url + "/identity-service/api/v1/Login"

    raw_data = {
        "username": idmcUsername,
        "password": idmcPassword,
    }

    headers = {
        "Content-type": "application/json"
    }

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
        logging.error(f"Error getting token: {tokenJson['error'][0]['message']}")
        exit(1)

    return tokenJson


def search_cdgc(searchTerm, segments, days = 9999):

    url = cdgc_api_url + "/data360/search/v1/assets?knowledgeQuery=" + searchTerm + "&segments=" + segments

    data = {
        "from": 0,
        "size": 100,
        "filterSpec": [
            {
                "type": "dsl",
                "expr": "core.CreatedOn within last " + str(days) + " day",
            }
        ]
    }

    headers = {
        "Content-type": "application/json",
        "X-INFA-ORG-ID": orgID,
        "Authorization": "Bearer " + token
    }

    data = json.dumps(data)

    response = requests.post(url, headers=headers, data=data)
    searchResults = response.text
    searchResults = json.loads(searchResults)

    return searchResults


def get_asset_relationship(assetID):

    url = cdgc_api_url + "/ccgf-searchv2/api/v1/search"

    logging.debug(f"Getting Relationships for asset ID = {assetID}")

    headers = {
        "Content-type": "application/json",
        "X-INFA-ORG-ID": orgID,
        "X-INFA-SEARCH-LANGUAGE": "elasticsearch",
        "Authorization": "Bearer " + token
    }

    body = {
       "from":0,
       "size":250,
       "query":{
          "bool":{
             "must":[
                {
                   "terms":{
                      "elementType":[
                         "RELATIONSHIP"
                      ]
                   }
                },
                {
                   "terms":{
                      "core.targetIdentity":[
                         assetID
                      ]
                   }
                }
             ],
             "filter":[

             ]
          }
       },
       "post_filter":{
          "bool":{
             "filter":[

             ]
          }
       }
    }

    body = json.dumps(body)

    response = requests.post(url, headers=headers, data=body)
    assetInfo = response.text

    logging.debug(f"Got Relationship Response")

    if response.status_code != 200:
        logging.error(f"Unexpected API Response code = " + str(response.status_code))
        exit(1)
    else:
        assetInfo = json.loads(assetInfo)

    return assetInfo


def delete_asset(assetID, assetClassType):

    global delete_count

    logging.debug(f"Attempting to delete an asset")

    url = cdgc_api_url + "/ccgf-contentv2/api/v1/publish"
    headers = {
        "Accept": "application/json",
        "Content-type": "application/json",
        "X-INFA-ORG-ID": orgID,
        "Authorization": "Bearer " + token,
        "IDS-SESSION-ID": sessionID,
        "X-INFA-PRODUCT-ID": "CDGC"
    }

    postData = {
        "items": [
            {
                "elementType": "OBJECT",
                "identity": assetID,
                "operation": "DELETE",
                "type": assetClassType,
                "identityType": "INTERNAL",
                "attributes": {

                }
            }
        ]
    }

    postData = json.dumps(postData)

    response = requests.post(url, headers=headers, data=postData)

    if response.status_code != 207:
        logging.warning(f"API Response code = " + str(response.status_code))
        print(response.text)
        return
    else:
        deleteResponse = response.text
        deleteResponse = json.loads(deleteResponse)

        if deleteResponse['items'][0]['messageCode'] == "CONTENT_FAILED":
            logging.debug(f"Status: " + deleteResponse['items'][0]['messageCode'])
            logging.debug(f"Reason: " + deleteResponse['items'][0]['validations'][0]['results'][0]['messageCode'])
        else:
            delete_count = delete_count + 1
            logging.debug(f"Asset has been deleted")

        return deleteResponse


def delete_relationship(assetLink):

    logging.debug(f"Attempting to delete a relationship")

    url = cdgc_api_url + "/ccgf-contentv2/api/v1/publish"
    headers = {
        "Accept": "application/json",
        "Content-type": "application/json",
        "X-INFA-ORG-ID": orgID,
        "Authorization": "Bearer " + token,
        "IDS-SESSION-ID": sessionID,
        "X-INFA-PRODUCT-ID": "CDGC"
    }

    for link in assetLink['sourceAsMap']['type']:

        body = {
            "items": [
                {
                 "elementType":"RELATIONSHIP",
                 "fromIdentity": assetLink['sourceAsMap']['core.sourceIdentity'],
                 "toIdentity": assetLink['sourceAsMap']['core.targetIdentity'],
                 "operation":"DELETE",
                 "type": link,
                 "identityType":"INTERNAL",
                 "attributes": {}
                }
            ]
        }

        body = json.dumps(body)

        response = requests.post(url, headers=headers, data=body)

        deleteResponse = response.text
        deleteResponse = json.loads(deleteResponse)

        if deleteResponse['items'][0]['messageCode'] == "CONTENT_FAILED":
            logging.debug(f"Status: " + deleteResponse['items'][0]['messageCode'])
            logging.debug(f"Reason: " + deleteResponse['items'][0]['validations'][0]['results'][0]['messageCode'])
        else:
            logging.debug(f"Relationship has been deleted")


def process_search_results(asset):

    # this function runs with  Threadpool for parallel execution and an asset array is passed, but each asset is processed individually

    logging.info(f"Asset : " + asset['summary']['core.name'])
    assetLinks = get_asset_relationship(asset['core.identity'])

    # If relationships exist, then go delete them. Might need to multi-thread this step at some point
    if assetLinks['hits']['total']['value'] > 0:
        logging.debug(f"Found " + str(assetLinks['hits']['total']['value']) + " Asset Links")
        for assetLink in assetLinks['hits']['hits']:
            delete_relationship(assetLink)

    # Delete the asset
    try:
        response = delete_asset(asset['core.identity'], asset['systemAttributes']['core.classType'])

    except Exception as e:
        logging.error(f"Error deleting asset")
        exit(1)


######################################################################################################
# Main
######################################################################################################
def main(idmcUsername, idmcPassword, days):

    global delete_count, orgID, token, sessionID
    assetType = "business assets"

    logging.info(f'Starting')

    # Login and set variable
    logging.info(f'Logging into IDMC')
    loginInfo = idmc_login(idmcUsername, idmcPassword, login_url)
    orgID = loginInfo['orgId']
    sessionID = loginInfo['sessionId']

    logging.info(f'Generate Access Token')
    tokenJson = generate_token(login_url)
    token = tokenJson['jwt_token']

    # keeps looping until no more objects are deleted
    while delete_count != 0:
        # reset this before we start our search and deletes
        delete_count = 0

        logging.info(f'Searching CDGC for asset type: {assetType}')
        searchResults = search_cdgc(assetType, "all", days)

        if "hits" in searchResults:
            logging.info(f"Found {searchResults['summary']['total_hits']} objects to delete")
            logging.info(f'Starting to delete assets')
            with ThreadPool(concurrentThreads) as pool:
                pool.imap_unordered(process_search_results, searchResults['hits'])
                pool.close()
                pool.join()
        else:
            logging.info(f"Found nothing to delete")

    logging.info(f'Script Completed')

if __name__ == "__main__":

    daysOld = 9999

    if ok_to_delete != "Y":
        print(f"Please update setup.py and set ok_to_delete to confirm it's ok to delete assets")
        sys.exit(2)

    # Set Parameters
    arg_help = f"""cdgc_delete_content.py -h -u <username> -p <password> -d <# of days>
           -h               help
           -u  <username>   Username to log into IDMC
           -p  <password>   Password to log into IDMC
           -d  <number>     Only delete assets that are specific number of days old
       """.format(sys.argv[0])

    # Fetch and Test Command Line Arguments
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hu:p:d:a:x", ["help", "username=", "password=", "days=", "debug"])

    except:
        print(arg_help)
        sys.exit(2)
    for opt, arg in opts:
        if opt in ("-h", "--help"):
            print(arg_help)  # print the help message
            sys.exit(2)
        elif opt in ("-u", "--username"):
            username = "Y"
        elif opt in ("-p", "--password"):
            password = "Y"
        elif opt in ("-d", "--days="):
            daysOld = arg
        elif opt in ("-x", "--debug"):
            logging.getLogger().setLevel(logging.DEBUG)
            logging.debug(f'Debug logging enabled')

    if not username or not password:
        print(f"Username or password was not provided. Please include it in your parameters or update setup.py")
        print(arg_help)
        sys.exit(2)

    main(username, password, daysOld)

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
# This script will attempt to purge all CDAM assets.
# This script is used in workshops and testing environments to clean slate an environment
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
        logging.error(f"Error getting token: {tokenJson['error']['message']}")
        exit(1)

    return tokenJson


def search_cdgc(searchTerm):


    url = f"{cdgc_api_url}/ccgf-searchv2/api/v1/search"

    data = {
       "from":0,
       "size":10000,
       "query":"*",
       "filter":[
          {
             "bool":{
                "filter":[
                   {
                      "terms":{
                         "core.classType":[
                            searchTerm
                         ]
                      }
                   }
                ],
                "must_not":[

                ]
             }
          }
       ],
       "aggs":{
          "core.classType":{
             "terms":{
                "field":"core.classType",
                "size":1000
             }
          },
          "core.resourceType":{
             "terms":{
                "field":"core.resourceType",
                "size":1000
             }
          },
          "core.origin":{
             "terms":{
                "field":"core.origin",
                "size":1000
             }
          },
          "core.assetLifecycle":{
             "terms":{
                "field":"core.assetLifecycle",
                "size":1000
             }
          },
          "core.createdBy":{
             "terms":{
                "field":"core.createdBy",
                "size":1000
             }
          },
          "core.modifiedBy":{
             "terms":{
                "field":"core.modifiedBy",
                "size":1000
             }
          },
          "core.stakeholderIdentity":{
             "terms":{
                "field":"core.stakeholderIdentity",
                "size":1000
             }
          }
       },
       "sort":[

       ],
       "function_score":{
          "functions":[
             {
                "filter":{
                   "term":{
                      "type":"core.IClassBusiness"
                   }
                },
                "weight":10
             },
             {
                "filter":{
                   "term":{
                      "type":"core.IClassTechnical"
                   }
                },
                "weight":5
             }
          ]
       }
    }

    headers = {
        "Authorization": "Bearer " + token,
        "Accept-Encoding": "gzip, deflate, br",
        "accept": "*/*",
        "content-type": "application/json",
        "X-INFA-SEARCH-LANGUAGE": "knowledge-graph-search",

    }

    data = json.dumps(data)

    response = requests.post(url, headers=headers, data=data)
    searchResults = response.text
    searchResults = json.loads(searchResults)

    return searchResults


def delete_asset(assetID, assetClassType):

    global delete_count

    logging.debug(f"Attempting to delete an asset id: {assetID}")

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


def process_search_results(asset):

    # this function runs with  Threadpool for parallel execution and an asset array is passed, but each asset is processed individually

    logging.info(f"Deleting Asset : " + asset['attributes']['core.name'])

    # Delete the asset
    try:
        response = delete_asset(asset['attributes']['core.identity'], asset['attributes']['core.classType'])

    except Exception as e:
        logging.error(f"Error deleting asset")
        exit(1)


######################################################################################################
# Main
######################################################################################################
def main(idmcUsername, idmcPassword, days):

    cdamAssets = ["DataAccessEnforcementPolicy", "DataFilterEnforcementPolicy", "DataProtection", "DataProtectionEnforcementPolicy", "PrecedenceTier"]

    global delete_count, orgID, token, sessionID

    logging.info(f'Starting Script')

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

        for assetType in cdamAssets:
            logging.info(f'Searching CDGC for asset type: {assetType}')
            searchTerm = f"com.infa.ccgf.models.cdam.{assetType}"
            searchResults = search_cdgc(searchTerm)

            if "hits" in searchResults:
                logging.info(f"Found {searchResults['hits']['total']['value']} objects to delete")
                logging.info(f'Starting to delete assets')
                with ThreadPool(concurrentThreads) as pool:
                    pool.imap_unordered(process_search_results, searchResults['hits']['hits'])
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
    arg_help = f"""cdgc_delete_cdam_assets.py -h -u <username> -p <password> -d <# of days>
           -h               help
           -u  <username>   Username to log into IDMC
           -p  <password>   Password to log into IDMC
       """.format(sys.argv[0])

    # Fetch and Test Command Line Arguments
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hu:p:a:x", ["help", "username=", "password=", "debug"])

    except:
        print(arg_help)
        sys.exit(2)
    for opt, arg in opts:
        if opt in ("-h", "--help"):
            print(arg_help)  # print the help message
            sys.exit(2)
        elif opt in ("-u", "--username"):
            username = arg
        elif opt in ("-p", "--password"):
            password = arg
        elif opt in ("-x", "--debug"):
            logging.getLogger().setLevel(logging.DEBUG)
            logging.debug(f'Debug logging enabled')

    if not username or not password:
        print(f"Username or password was not provided. Please include it in your parameters or update setup.py")
        print(arg_help)
        sys.exit(2)

    main(username, password, daysOld)

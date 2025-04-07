import json
import requests
import sys
import getopt
from setup import *
from datetime import datetime, timedelta
from urllib.parse import urlparse
import logging

# ---------------------------------------------------------------------------------------------------------------------------------------------
# Overview - This script allows you to send a notification to all users, a specific user, or users in a group or role. This notification will
# show up in the little Bell section of the app and then be viewable on the Notifications page.
# ---------------------------------------------------------------------------------------------------------------------------------------------

loglevel = 1
notifyType = ""

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)-5s - %(message)s'
)

# ---------------------------------------------------------------------------------------------------------------------------------------------
# Functions
# ---------------------------------------------------------------------------------------------------------------------------------------------
def idmc_login(idmcUsername, idmcPassword, url):

    url = url + "/ma/api/v2/user/login"

    raw_data = '{"username": "' + idmcUsername + '","password": "' + idmcPassword + '"}'
    headers = {'Content-type': 'application/json'}

    response = requests.post(url, data=raw_data, headers=headers)
    loginInfo = response.text
    loginInfo = json.loads(loginInfo)

    return loginInfo


def generate_token(sessionID, url):
    url = url + "/identity-service/api/v1/jwt/Token?client_id=cdlg_app&nonce=1234"
    headers = {'Content-type': 'application/json', 'cookie': 'USER_SESSION=' + sessionID, 'IDS-SESSION-ID': sessionID}

    response = requests.post(url, headers=headers)
    tokenJson = response.text
    tokenJson = json.loads(tokenJson)

    return tokenJson


def get_user_id(sessionID, url, bearToken, userName):

    url = "https://" + url + "/saas/public/core/v3/users?q=userName==" + userName

    headers = {'Content-type': 'application/json', 'INFA-SESSION-ID': sessionID, 'Authorization': 'Bearer ' + bearToken}

    response = requests.get(url, headers=headers)
    response = response.text
    response = json.loads(response)

    if response:
        return response[0]['id']
    else:
        return ""


def get_group_id(sessionID, url, bearToken, groupName ):

    url = "https://" + url + "/saas/public/core/v3/userGroups?q=userGroupName=='" + groupName + "'"

    headers = {'Content-type': 'application/json', 'INFA-SESSION-ID': sessionID,
               'Authorization': 'Bearer ' + bearToken}

    response = requests.get(url, headers=headers)
    response = response.text
    response = json.loads(response)
    if response:
        return response[0]['id']
    else:
        return ""


def idmc_msg_bell(serverHost, bearToken, sessionID, title, expires, orgID, productID, userID, roleName, userGroupID, message, linkTest, priority, urlLink, statusLevel):

    url = "https://" + serverHost + "/notification-service/api/v1/Messages"

    msg = {
        "content": title,
        "contentType": "text/plain",
        "expires": expires + "T12:00:00.000-07:00",
        "locale": "en",
        "orgId": orgID,
        "messagePriority": priority,
        "messageType": "BELL_NOTIFICATION",
        "productId": productID,
        "severity": "INFO",
        "recipients": {
        },
        "x-headers": {
            "notificationDetailsBaseUrl": urlLink,
            "name": linkTest,
            "description": message,
            "status": statusLevel
        }
    }

    if userID:
        msg["recipients"] = { "userIds": [ userID ] }

    if roleName:
        msg["recipients"] = {"roleNames": [ roleName ] }

    if userGroupID:
        msg["recipients"] = { "userGroupIds": [ userGroupID ] }

    msg = "[" + json.dumps(msg) + "]"

    headers = {'Content-type': 'application/json', 'xsrf_token': 'custom_msg',
               'Cookie': 'XSRF_TOKEN=custom_msg; USER_SESSION=' + sessionID, 'Authorization': 'Bearer ' + bearToken}

    response = requests.post(url, data=msg, headers=headers)
    response = response.text
    response = json.loads(response)
    return response

# ---------------------------------------------------------------------------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------------------------------------------------------------------------
def main(argv):

    title, message, linktext, url, userName, roleName, userGroupName, userID, userGroupID = "", "", "", "", "", "", "", "", "",
    expireDays = 1
    productID = "ccgf.apps.cdlg"
    priority = "LOW"
    statusLevel = "INFO"
    sendAll = "N"

    # Set Parameters
    arg_help = f"""idmc_send_notifications.py
        -h              help
        -t <title>      The Title of the Notification (required)
        -m <message>    The Message (required)
        -u <url>        URL that is available right under the message (optional)
        -l <link text>  The text for the URL (optional)
        -p <priority>   Priority of the Bell Notification (optional: LOW, HIGH)
        -s <status>     Changes the little icon in front of the message (optional: INFO, ERROR, SUCCESS, WARNING)
        ---- requires one of these ----
        -e <username>   The IDMC username of the user to receive the notification
        -r <role>       Send users with this role the notification 
        -g <group>      Send users that belong to this group the notification
        -a              Send to all users
        
    """.format(argv[0])

    try:
        opts, args = getopt.getopt(argv[1:], "ht:m:x:u:l:p:s:e:ar:g:", ["title=","message=","expire=","linktext=","priority=","url=","status=", "username=", "all", "role=", "group="])

    except getopt.GetoptError as err:
        print(arg_help)
        sys.exit(2)

    if not opts:
        print(arg_help)
        sys.exit(2)

    for opt, arg in opts:
        if opt in ("-h", "--help"):
            print(arg_help)  # print the help message
            sys.exit(2)
        elif opt in ("-t", "--title"):
            title = arg
        elif opt in ("-m", "--message"):
            message = arg
        elif opt in ("-x", "--expire"):
            expireDays = arg
        elif opt in ("-l", "--linktext"):
            linktext = arg
        elif opt in ("-p", "--priority"):
            priority = arg
        elif opt in ("-u", "--url"):
            url = arg
        elif opt in ("-s", "--status"):
            statusLevel = arg
        elif opt in ("-e", "--username"):
            userName = arg
        elif opt in ("-r", "--rolename"):
            roleName = arg
        elif opt in ("-g", "--group"):
            userGroupName = arg
        elif opt in ("-a", "--all"):
            sendAll = "Y"

    if sendAll == "N" and not userName and not roleName and not userGroupName:
        print("You must include one type of user to send notification too!")
        print(arg_help)
        sys.exit(2)

    logging.info("Starting")
    logging.info("Parameters")
    logging.info("    - title: " + title)
    logging.info("    - message: " + message)
    logging.info("    - priority: " + priority)
    logging.info("    - status: " + statusLevel)
    logging.info("    - link: " + url)
    logging.info("    - link text: " + linktext)
    logging.info("    - expire in days: " + str(expireDays))
    if userName:
        logging.info("    - User to Notify: " + userName)
    if userGroupName:
        logging.info("    - Group to Notify: " + userGroupName)
    if roleName:
        logging.info("    - Role to Notify: " + roleName)

    if sendAll == "Y":
        logging.info("    - User to Notify: ALL")

    # Login and set variables
    logging.info("Logging in")
    loginInfo = idmc_login(username, password, login_url)

    orgID = loginInfo['orgUuid']
    serverHost = loginInfo['serverUrl']
    serverHost = urlparse(serverHost).hostname
    sessionID = loginInfo['icSessionId']

    logging.info("    -  Org ID = " + orgID)
    logging.info("    -  IDMC Host = " + serverHost)

    logging.info("Getting Token")
    tokenJson = generate_token(sessionID, login_url)
    bearToken = tokenJson['jwt_token']

    if userName:
        userID = get_user_id(sessionID, serverHost, bearToken, userName)

    if userGroupName:
        userGroupID = get_group_id(sessionID, serverHost, bearToken, userGroupName)

    logging.info("Sending Notification")

    today = datetime.today()
    today = today + timedelta(days=int(expireDays))
    expires = today.strftime('%Y-%m-%d')

    if userID or sendAll == "Y" or roleName or userGroupID:
        idmc_msg_bell(serverHost, bearToken, sessionID, title, expires, orgID, productID, userID, roleName, userGroupID, message, linktext, priority, url, statusLevel)

    logging.info("Finished")


if __name__ == "__main__":
    main(sys.argv)

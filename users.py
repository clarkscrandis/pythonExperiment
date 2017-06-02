import sys
import json
#from collections import namedtuple
from elasticsearch import Elasticsearch
import pprint #PrettyPrinter

REMOTE_DBG = False
ES_HOST = 'localhost'
ES_PORT = 9200

def findUserQuery(emailValue):
    return { 'query': {
                        'constant_score': {
                            'filter': {
                                'term': {
                                    'email': emailValue
                                }
                            }
                        }
                }
            }

m_pp = pprint.PrettyPrinter(indent=4)
m_es = Elasticsearch([{'host':ES_HOST, 'port':ES_PORT}])

# python users.py add "[{\"firstName\":\"bogusFirstName\",\"lastName\":\"bogusLastName\",\"email\":\"name@company.com\"}]"
# NOTE: email needs to be unique among users. If a user already has the provided email address, then a new user will NOT be created. 
def addUsers(jsonTxt):
    userList = json.loads(jsonTxt)
    #x = json.loads(jsonTxt, object_hook=lambda d: namedtuple('X', d.keys())(*d.values()))
    #print x.email
    response = { 'addedUserList' : [],
                 'existingUserList' : [],
                 'errorUserList' : []
        }
    
#    b = {"userId": "0"}
    for user in userList:
        searchRes = m_es.search(index='user', doc_type='userType', body=findUserQuery(user['email']))
        #m_pp.pprint(searchRes)
        if searchRes['hits']['total']:
            #User IS already in the db
            user['userId'] = searchRes['hits']['hits'][0]['_id']
            response['existingUserList'].append(user)
        else:
            #User is not yet in the db
            res = m_es.index(index='user', doc_type='userType', body=user)
            if res['created'] == True:
                user['userId'] = res['_id']
                response['addedUserList'].append(user)
            else:
                response['errorUserList'].append(user)
    jsonTxt2 = json.dumps(response)
#    print addObject[0]['key']
    return (jsonTxt2)
    
# python users.py delete "[{\"userId\":\"123\"}]"
def deleteUsers(jsonTxt):
    response = { 'deletedUserIdList' : [],
                 'missingUserIdList' : [],
                 'errorUserIdList' : []
        }
    
    #idList = json.loads(jsonTxt, object_hook=lambda d: namedtuple('X', d.keys())(*d.values()))
    userList = json.loads(jsonTxt)
    
    for user in userList:
        #Might not need this try/except logic if we upgrade to ES 5.x
        try:
            res = m_es.delete(index='user', doc_type='userType', id=user['userId'], refresh=True)
            #m_pp.pprint(res)
            if res['found'] == True:
                response['deletedUserIdList'].append(user)
            else:
                response['missingUserIdList'].append(user)
        except:
            response['errorUserIdList'].append(user) # ES 2.2.0 issues a NotFoundError if the specified Id is not in the DB...so this list currently contains errors as well as missing Ids. 
    responseTxt = json.dumps(response)
    return responseTxt
    
#python users.py getAll "{}"
def getAllUsers():
    findAllUsersQuery = { 'query': { 'match_all': {} } }

    response = { 'foundUserList' : [],
                 'missingUserIdList' : [],
                 'errorUserIdList' : []
        }
    
    searchRes = m_es.search(index='user', doc_type='userType', body=findAllUsersQuery)
    if searchRes['hits']['total'] > 0:
        userList = searchRes['hits']['hits']
        for user in userList:
            user['_source']['userId'] = user['_id']
            response['foundUserList'].append(user['_source'])   
    jsonTxt2 = json.dumps(response)
    return jsonTxt2

# python users.py get "[{\"userId\":\"123\"}]"
def getUsers(jsonTxt):
    response = { 'foundUserList' : [],
                 'missingUserIdList' : [],
                 'errorUserIdList' : []
        }
    
    #idList = json.loads(jsonTxt, object_hook=lambda d: namedtuple('X', d.keys())(*d.values()))
    idList = json.loads(jsonTxt)
    for user in idList:
        #Might not need this try/except logic if we upgrade to ES 5.x
        try:
            res = m_es.get_source(index='user', doc_type='userType', id=user['userId'])
            #m_pp.pprint(res)
            res.update(user)
            response['foundUserList'].append(res)
        except:
            response['missingUserIdList'].append(user)
    jsonTxt2 = json.dumps(response)
    return jsonTxt2
    
# Confirm ES is running and if the user index not yet created, set up the appropriate mapping
#python users.py init "{}"
def initUserStore():
    responseTxt = 'NOT OK'
    userTypeMapping = {
      "mappings": {
        "userType" : {
          "properties" : {
            "companyId" : {
              "type" :    "string"
            },
            "contractIdSet" : {
              "type" :    "string"
            },
            "email" : {
              "type" :   "string",
              "index" : "not_analyzed"
            },
            "firstName" : {
              "type" :    "string"
            },
            "lastName" : {
              "type" :   "string"
            },
            "permissionSet" : {
              "type" :   "string"
            },
            "searchIdSet" : {
              "type" :    "string"
            },
            "resumeSet" : {
              "type" :    "string"
            },
            "roleSet" : {
              "type" :    "string"
            }
          }
        }
      }
                        }
    try:
        userStoreExists = m_es.indices.exists(index='user')
        responseTxt = 'OK'
    except Exception, e:
        sys.stderr.write("Please fix connection to Elasticsearch: " + ES_HOST + ":" + str(ES_PORT) + "\n")
        sys.stderr.write(str(e))
        sys.exit(1)
    if not(userStoreExists):
        res = m_es.indices.create(index='user', body=userTypeMapping, refresh=True)
        if res['acknowledged'] == True:
            responseTxt = 'OK'
        else:
            sys.stderr.write("Unable to define the mapping needed by the user store\n")
            sys.exit(1)
    return responseTxt

# python users.py update "{\"userId\":\"123\", \"firstName\":\"bogusFirstName\",\"lastName\":\"bogusLastName\",\"email\":\"name@company.com\", \"permissionSet\":[\"str3\", \"str2\"]}"
# Update the list of fields that are provided. If you want to remove fields, then a different function will be necessary.
# NOTE: I expect the webserver/user will correctly format and structure the json that is provided.
def updateUser(jsonTxt):
    #TODO: Perform error handling if there is a problem with adding the data back in
    updatedUser = json.loads(jsonTxt)
    
    # Extract and remove userId out of the user record.
    userId = updatedUser['userId']
    del updatedUser['userId']

    try:
        # Get the current user record and update it with the values that were provided.
        user = m_es.get_source(index='user', doc_type='userType', id=userId)
        user.update(updatedUser)
     
        # Write the user record back out.
        m_es.index(index='user', doc_type='userType', body=user, id=userId, refresh=True) 
    except Exception, e:
        sys.stderr.write("Unable to find or update the specified user: " + userId + "\n")
        sys.stderr.write(str(e))
        sys.exit(1)

    return (jsonTxt)
    
    
if __name__ == '__main__':
    if REMOTE_DBG:
        # REMOTE_DBG is defined in constants.py
        # For more information about remote debugging, see: http://kodi.wiki/view/HOW-TO:Debug_Python_Scripts_with_Eclipse
        try:
            sys.path.append('full_path_to_pysrc')
            import pydevd
            # stdoutToServer and stderrToServer redirect stdout and stderr to eclipse console
            pydevd.settrace('localhost', port=5678, stdoutToServer=True, stderrToServer=True)
        except ImportError:
            sys.stderr.write("Error: " +
                "You must add org.python.pydev.debug.pysrc to your PYTHONPATH.")
            sys.exit(1)
            
    if len(sys.argv) >= 3:
        #print ('python says: '+ sys.argv[1])
        func = sys.argv[1]
        inputData = sys.argv[2]
    else:
        sys.stderr.write('You need to pass at least three values to python: [module] [command] [json data]\n')
        sys.exit(1)
    
    if func == 'add':
        print(addUsers(inputData))
    elif func == 'delete':
        print(deleteUsers(inputData))
    elif func == 'getAll':
        print(getAllUsers())
    elif func == 'get':
        print(getUsers(inputData))
    elif func == 'init':
        print(initUserStore())
    elif func == 'update':
        print(updateUser(inputData))
    else:
        sys.stderr.write('You specified an unsupported command: ' + func)
        sys.exit(1)

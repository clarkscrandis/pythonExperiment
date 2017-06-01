import sys
import json
from collections import namedtuple
from elasticsearch import Elasticsearch
import pprint #PrettyPrinter

REMOTE_DBG = False
ES_HOST = 'localhost'
ES_PORT = 9200

# Call this from the commandline using the following syntax: python users.py add "[{\"key\":\"value1\"}, {\"key\":\"value2\"}]"
# python users.py add "[{\"firstName\":\"value1\"}]"

findAllUsersQuery = { 'query': { 'match_all': {} } }

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

# Confirm ES is running and if the user index not yet created, set up the appropriate mapping
#python users.py init "{}"
def initUserStore():
    responseTxt = 'NOT OK'
    userTypeMapping = {
      "mappings": {
        "userType" : {
          "properties" : {
            "firstName" : {
              "type" :    "string"
            },
            "lastName" : {
              "type" :   "string"
            },
            "email" : {
              "type" :   "string",
              "index" : "not_analyzed"
            }
          }
        }
      }
                        }
    try:
        userStoreExists = m_es.indices.exists(index='user')
        responseTxt = 'OK'
    except:
        sys.stderr.write("Please fix connection to Elasticsearch: " + ES_HOST + ":" + str(ES_PORT) + "\n")
        sys.exit(1)
    if not(userStoreExists):
        res = m_es.indices.create(index='user', body=userTypeMapping)
        if res['acknowledged'] == True:
            responseTxt = 'OK'
        else:
            sys.stderr.write("Unable to define the mapping needed by the user store")
            sys.exit(1)
    return responseTxt

# python users.py add "[{\"firstName\":\"bogusFirstName\",\"lastName\":\"bogusLastName\",\"email\":\"name@company.com\"}]"
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
            res = m_es.delete(index='user', doc_type='userType', id=user['userId'])
            #m_pp.pprint(res)
            if res['found'] == True:
                response['deletedUserIdList'].append(user)
            else:
                response['missingUserIdList'].append(user)
        except:
            response['errorUserIdList'].append(user) # ES 2.2.0 issues a NotFoundError if the specified Id is not in the DB...so this list currently contains errors as well as missing Ids. 
    responseTxt = json.dumps(response)
    return responseTxt
    
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
    
#python users.py getAll "{}"
def getAllUsers():
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
    elif len(sys.argv) == 2:
        print ('in python')
        #inputData = sys.argv[1]['args'][0]
        #print func
        #print inputData
        sys.exit()
    else:
        sys.stderr.write('You didn\'t provide enough values')
        sys.exit(1)
    
    if func == 'add':
        print(addUsers(inputData))
    elif func == 'delete':
        print(deleteUsers(inputData))
    elif func == 'get':
        print(getUsers(inputData))
    elif func == 'getAll':
        print(getAllUsers())
    elif func == 'init':
        print(initUserStore())
    else:
        sys.stderr.write('You asked an unsupported function: ' + func)
        sys.exit(1)

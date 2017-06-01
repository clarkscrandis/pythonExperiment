import sys
import json
from collections import namedtuple
from elasticsearch import Elasticsearch
import pprint #PrettyPrinter

REMOTE_DBG = False

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
m_es = Elasticsearch([{'host':'localhost', 'port':9200}])


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
        #pp.pprint(findUserQuery(user['email']))
        searchRes = m_es.search(index='user', doc_type='userType', body=findUserQuery(user['email']))
        if searchRes['hits']['total']:
            #User IS already in the db
            user['userId'] = searchRes['hits']['hits'][0]['_id']
            #m_pp.pprint(searchRes)
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
    
def deleteUsers(jsonTxt):
    return ('Data passed to deleteUsers: ' + jsonTxt)
    
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
    else:
        sys.stderr.write('You asked an unsupported function: ' + func)
        sys.exit(1)

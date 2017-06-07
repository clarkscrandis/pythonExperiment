import sys
import json
from users import countUsers
import pprint #PrettyPrinter

REMOTE_DBG = False

#python counts.py getAll "{}"
def getAllCounts():

    response = { 
        'users' : 0,
        'resumes' : 0
    }
    
    response['users'] = countUsers()
    response['resumes'] = 0
       
    jsonTxt2 = json.dumps(response)
    return jsonTxt2

# python counts.py get "[\"users\",\"resumes\"]"
def getCounts(jsonTxt):
    response = {}
    
    objList = json.loads(jsonTxt)
    for obj in objList:
        if obj == 'users':
            response['users'] = countUsers()
        elif obj == 'resumes':
            response['resumes'] = 0
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
    else:
        sys.stderr.write('You need to pass at least three values to python: [module] [command] [json data]\n')
        sys.exit(1)
    
    if func == 'getAll':
        print(getAllCounts())
    elif func == 'get':
        print(getCounts(inputData))
    else:
        sys.stderr.write('You specified an unsupported command: ' + func)
        sys.exit(1)


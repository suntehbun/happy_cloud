import requests
from flask import Flask, request, session, url_for, redirect, \
     render_template, abort, g, flash, _app_ctx_stack, jsonify
#help with merging dicts
from itertools import chain
from collections import defaultdict

app = Flask(__name__)


'''
1. Name node
this does the things

'''
#metadata structure
MDS = {}

#structure to hold the datanode and ips?
datanodes = {}

#Blockname the parts
def BlockDivide(size):
    num_blocks = 67108864/size
    return num_blocks

def CheckJsonValid(json):
    error = json.get('error')
    #establish valid number whatever it is
    if error == -1:
        return True
    else:
        return False

def CreateInvalidJson():
    response = {'valid': -1, 'message': 'not valid json'}
    return response

#checks if file exists and returns a json
def CheckFile(Recvjson):
    #get the file path from the json here
    ClientFilePath = Recvjson.get('path')
    #are we getting the file or the path?
    if ClientFilePath not in MDS:
        #put into json response
        num_of_blocks= BlockDivide()
        #return blocknames/list of blocks, and data nodes
        dictoReturn = {'valid': 0, 'message': 'here are some things'}
        #get list of datanodes here
        dictoReturn.update({'datenodes': datanodelist})
        dictoReturn.update({'blocks': num_of_blocks})
        return jsonify(dictoReturn)
    else:
        dictoReturn = {'valid': -1, 'message': 'File already Exists'}
        return jsonify(dictoReturn)
#checks if the directory exists in our file system
#wip
def CheckDir( dir ):
    #this storoes the current filepaths available
    currentKeys = list(MDS.keys())
    #to store the parts of unique n+1 file path
    contents = set()
    for key in currentKeys:
        #split the file path and store for easier finding
        key.split("/")
        keylength = len(key)
        for i in range (0, keylength):
            #to avoid buffer overflow
            if key[i] == dir and i != keylength-1 :
                #stores only the unique folder dir afterwards
                contents.add(key[i+1])
    #unique list
    return contents


@app.route('/')
def hello_world():
	return 'connected to name node'

#don't think its possible for a get since client would always give us info
@app.route('/create_file', methods=['GET', 'POST'])
#need to redo this for json interactions
def create_file():
    error = None
    #not sure about this yets
    if request.method == 'POST':
        print("Client sending a file path")
	    #get the json file
        Recvjson= request.get_json(force=True)
        #print to test if recieved json
        print ('data from client: ' Recvjson)
        #if valid json
        valid = CheckJsonValid(Recvjson)
        if valid:
            #returns a dict of our data
            dictoReturn=CheckFile(Recvjson)
        #fix logic here
        else:
            print('not valid json code')
            #replace this with a client ip getter
            #res = requests.post('http://35.161.253.213:5000/tests/endpoint', json=dataToSend)
            #print ('response from data node:',res.text)
            dictFromServer = CreateInvalidJson()
            return jsonify(dictFromServer)
    else:
        return 'some other method'


@app.route('/read_file', methods=['GET', 'POST'])
def read_file():
    error = None
        if request.method == 'POST':
            filepath = #filepath from JSON
            #assume '_sequenceNo_' precedes numbered file path piece
            #for example library/doc/image.jpg_sequenceNo_3 this is the 3rd piece of image.jpg
        fileNames =[]
            #each list in list is list of dataNodes holding that data block
            fileLoc = MDS[filepath]
            fileBlockSize = len(fileLoc)
            #populate fileNames list with the names of Blocks stored in dataNodes
        for i in range(o,n):
                fileNames.append(filepath+'_sequenceNo_'+i)
            #need to send fileLoc and fileNames to client
            nameDic = {'name':fileNames}
            locationDic = {'location':fileLoc}
        return render_template('read_file.html', error=error)

#this might be a socket programming thing.
@app.route('/receive_block_reports')
def receive_block_reports():
    #not sure what format these are in but logic is like this
    if (valid message or whatever):
        NewMDS=defaultdict(list)
        blockreport = message.getdict
        if file in MDS:
            tempMDS = MDS
            for k, v in chain(blockreport.items(), tempMDS.items()):
                NewMDS[k].append(v);
            MDS=NewMDS
            return 'successfully added'
        else:
            MDS.update(blockreport)
    else:
        return 'invalid blockreport'

#will need to discuss about this.
@app.route('/delete_file', methods=['GET', 'POST'])
def delete_file():


@app.route('/create_directory', methods=['GET', 'POST'])
#replace this with the above function check dir
def create_directory():

#will need to disscuss about this
@app.route('/delete_directory', methods=['GET', 'POST'])
def delete_directory():


@app.route('/list_datanodes', methods=['GET', 'POST'])
def list_datanodes():
	error = None
	if request.method == 'POST':
		# send file path to the data node
		call_function()
	return render_template('list_datanodes.html', error=error)

app.secret_key = 'A0Zr98j/3yX R~XHH!jmN]LWX/,?RT'

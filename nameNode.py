import requests
from flask import Flask, request, session, url_for, redirect, \
     render_template, abort, g, flash, _app_ctx_stack, jsonify


app = Flask(__name__)


'''
1. Name node
this does the things

'''
#metadata structure
MDS = {}

def CheckIfFileExist( file ):
    

#Blockname the parts
def BlockName():

#checks if the directory exists in our file system
#wip
def CheckDir( dir ):
    #this storoes the current filepaths available
    currentKeys = list(MDS.keys())
    #to store the parts of unique n+1 file path
    contents = set()
    for key in currentKeys:
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
	return render_template('layout.html')


@app.route('/create_file', methods=['GET', 'POST'])
def create_file():
	error = None
	if request.method == 'POST':
		print("got here")
		# send file path to the data node
		print (request.form['filepath'])
		dataToSend = {'filepath':request.form['filepath']}
		print('got here 2')
		res = requests.post('http://35.161.253.213:5000/tests/endpoint', json=dataToSend)
		print ('response from data node:',res.text)
		dictFromServer = res.json()
		return 'SUCCESSFUL HANDSHAKE!'
	else:
		return render_template('create_file.html', error=error)


@app.route('/read_file', methods=['GET', 'POST'])
def read_file():
	error = None
	if request.method == 'POST':
		# send file path to the data node
		call_function()
	return render_template('read_file.html', error=error)

@app.route('/delete_file', methods=['GET', 'POST'])
def delete_file():
	error = None
	if request.method == 'POST':
		# send file path to the data node
		call_function()
	return render_template('delete_file.html', error=error)

@app.route('/create_directory', methods=['GET', 'POST'])
def create_directory():
	error = None
	if request.method == 'POST':
		# send file path to the data node
		call_function()
	return render_template('create_directory.html', error=error)

@app.route('/delete_directory', methods=['GET', 'POST'])
def delete_directory():
	error = None
	if request.method == 'POST':
		# send file path to the data node
		call_function()
	return render_template('delete_directory.html', error=error)

@app.route('/list_contents', methods=['GET', 'POST'])
def list_contents():
	error = None
	if request.method == 'POST':
		# send file path to the data node
		call_function()
	return render_template('list_contents.html', error=error)

@app.route('/list_datanodes', methods=['GET', 'POST'])
def list_datanodes():
	error = None
	if request.method == 'POST':
		# send file path to the data node
		call_function()
	return render_template('list_datanodes.html', error=error)

app.secret_key = 'A0Zr98j/3yX R~XHH!jmN]LWX/,?RT'

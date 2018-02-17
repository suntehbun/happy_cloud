import requests
from flask import Flask, request, session, url_for, redirect, \
     render_template, abort, g, flash, _app_ctx_stack, jsonify


app = Flask(__name__)


'''
1. Create a new file in SUFS
when the file is created, an S3 object should be specified and the data from S3 should be written into the file
2. Read a file
the file will be read from SUFS and a copy of the file is returned
3. Delete a file
4. Create a directory
5. Delete a directory
6. List the contents of a directory
7. List the DataNodes that store replicas of each block of a file
be sure to keep the output from this somewhat neat - it could get long for large files (i.e., many blocks)
'''

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


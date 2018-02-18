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

# metadata data structure
metadata = {}


@app.route('/')
def hello_world():
	return 'connected to the name node'


# get
@app.route('/receive_file', methods=['GET'])
def receive_file():
	# check if the path is unique
	input_json = request.get_json(force=True)  
	print 'data from client:', input_json
	path = input_json.get('path')
	size = input_json.get('size')
    if path in metadata:
    	dataToReturn = {'valid': -1}
    	return jsonify(dataToReturn)
    else:
    	metadata[path] = [] # store in the metadata
    	num_blocks = size / 67108864
    	# contact ec2 and see they are avaiable 
    	# pick avaiable block addresses
    	dataToReturn = {'num_blocks': num_blocks, 'addresses': [, , ,]}
    	return jsonify(dataToReturn)

# get block report from data nodes



app.secret_key = 'A0Zr98j/3yX R~XHH!jmN]LWX/,?RT'


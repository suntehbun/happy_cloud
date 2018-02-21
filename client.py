import requests, boto3, botocore, os, math
from flask import Flask, request, session, url_for, redirect, \
     render_template, abort, g, flash, _app_ctx_stack, jsonify
from subprocess import call



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
with open(S3Path, 'wb') as data:
    			obj.download_fileobj(data)
    			#fo = open('S3_downloaded_file', 'rw+')
			#fo.readlines(500000) # num bytes
'''

#global variables
NAMENODE_ADDRESS = '54.218.46.77:5000'

# function that extracts key and name from S3 file path
def extractS3Info(S3Link):
	print('extractS3Info called')

	linkLength = len(S3Link)
	keyPosition = linkLength - 1
	bucketPosition = 0
	S3Key = ''
	S3Bucket = ''

	while bucketPosition != len(S3Link):
		if S3Link[bucketPosition] != '/':
			S3Bucket += S3Link[bucketPosition]
			bucketPosition += 1
		else:
			bucketPosition += 1
			break
	print('S3bucket: ', S3Bucket)

	while bucketPosition != linkLength:
		S3Key += S3Link[bucketPosition] 
		bucketPosition += 1

	'''
	while keyPosition != 0:
		if S3Link[keyPosition] != '/':
			S3Key = S3Link[keyPosition] + S3Key
			keyPosition -= 1
		else: 
			break
	'''
	print('S3Key: ', S3Key)
	'''
	bucketPosition = keyPosition - 1
	while bucketPosition != 0:
		if S3Link[bucketPosition] != '/':
			S3Bucket = S3Link[bucketPosition] + S3Bucket
			bucketPosition -= 1
		else:
			break
	
	print('S3bucket: ', S3Bucket)
	'''
	S3Info = {'S3Key' : S3Key, 'S3Bucket': S3Bucket}
	return S3Info

def splitFileInBlocks(newFilePath, key):

	fid = 1
	with open(newFilePath) as infile:
		def readOneBlock():
			return infile.read(67108864) #test with different value
		for block in iter(readOneBlock, ''):
			print ('in one iteration in splitFileInBlock')
			newFileName = key + '_' +str(fid) + '.txt'
			print('newFileName: ', newFileName)
			f = open('file%d.txt' %fid, 'w')
			#f = open(newFileName, 'w')
			f.write(block)
			f.close()
			fid += 1


@app.route('/')
def hello_world():
	return render_template('layout.html')

# this function should transfer S3 bucket data and divide them to each block that is received from the name node
# client should run locally and can save whatever file they want, but to eliminate additinoal fee charge from data tranferring from outside of aws to aws,
# we decided to implement client in aws and get S3 file 

# accept S3 file path only for simplicity and efficiency 
@app.route('/create_file', methods=['GET', 'POST'])
def create_file():
	error = None
	if request.method == 'POST':
		# send file path to the data node
		print (request.form['filepath'])
		print(request.form['S3filepath'])

		filepath = request.form['filepath']
		S3Path = request.form['S3filepath']
		# fileSize = os.stat().st_size Need to Know the File Size!!!
		# so should I download the S3 File first??
		S3Info = extractS3Info(S3Path)

		BUCKET_NAME = S3Info['S3Bucket'] # replace with your bucket name
		KEY = S3Info['S3Key'] # replace with your object key
		print('Bucket name: ', BUCKET_NAME)
		print('Key: ', KEY)

		#1. Download file from S3
		s3 = boto3.resource('s3')

		for bucket in s3.buckets.all():
			print(bucket.name)

		newFilePath = '/home/ec2-user/SUFS/venv/S3Download/downloaded' # + KEY
		#newFilePath = '/home/ec2-user/SUFS/venv/S3Download/' + KEY
		print('newFilePath: ', newFilePath)

		try:
			s3.Bucket(BUCKET_NAME).download_file(KEY, newFilePath)
		except botocore.exceptions.ClientError as e:
			if e.response['Error']['Code'] == '404':
				return render_template('create_file.html', data={'valid': -1, 'status': 'File does not exist in S3'})
			else:
				return render_template('create_file.html', data={'valid': -1, 'status': 'S3 Bucket Doesnt exist'})

		S3FileSize = os.stat(newFilePath).st_size
		print('downloaded file sizeL: ',  S3FileSize)

		
		#2. contact to data node and check if filepath exists, gets num blocks if it doesn't exist
		dataToSend = {'filePath':request.form['filepath'], 'fileSize': S3FileSize} # this path should be a path where client wants to save data in 
		namenodeCreateFilePath = 'http://' + NAMENODE_ADDRESS + '/create_file'
		print('namenodeCreateFilePath: ', namenodeCreateFilePath)
		
		try:
			res = requests.get(namenodeCreateFilePath, json=dataToSend) # contact namenode to get num of blocks and data node addressees
		except requests.exceptions.RequestException as e:
			return render_template('create_file.html', data={'valid': -1, 'status': 'Name node connection error'})
		print ('response from data node:',res.text)
		namenodeData = res.json()
		

		# check if the file already exists or not
		isFileExists = namenodeData['valid']
		
		isFileExists = 1 # delete this later: testing purpose
		if isFileExists == -1:
			return render_template('create_file.html', data={'valid': -1, 'status': 'File Already Exists'})
		else:
			#3. Split the file into blocks
			splitFileInBlocks(newFilePath, KEY)
		

		#3. Split the file into blocks
		#splitFileInBlocks(newFilePath)

		#4. Store the block in Data nodes (contact ec2 with blocks)
		
		blockNums = len(namenodeData['addresses'])
		fid = 1
		print('block nums: ', blockNums)
		#cmd = 'scp -i aKey.pem aFile.txt ec2-user@serverIp:folder'
		#cmd = 'scp -i /home/ec2-user/SUFS/venv/key/HaileysKey.pem aFile.txt ec2-user@serverIp:folder'
		'''
		fileTransferCommand = 'scp -i /home/ec2-user/SUFS/venv/key/HaileysKey.pem file' + str(fid) + '.txt ec2-user@172.31.25.233:/home/ec2-user/SUFS/venv/datanode'
		print('fileTransferCommand: ', fileTransferCommand)
		call(fileTransferCommand.split())
		'''
		
		for address in namenodeData['addresses']:
			#fileName = 'file%d.txt'
			fileName = KEY + str(fid)
			print('file name before sending it to the data node: ', fileName)
			fileTransferCommand = 'scp -i /home/ec2-user/SUFS/venv/key/HaileysKey.pem ' + fileName +' ec2-user@' + address + ':/home/ec2-user/SUFS/venv/datanode'
			print('fileTransferCommand: ', fileTransferCommand)
			try:
				call(fileTransferCommand.split())
			except requests.exceptions.RequestException as e:
				return render_template('create_file.html', data={'valid': -1, 'status': 'Data node error!'})

		return render_template('create_file.html', data={'valid': 1, 'status': 'Stored Successfully!'})
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


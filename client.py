import requests, boto3, botocore, os, math, glob
from flask import Flask, request, session, url_for, redirect, \
     render_template, abort, g, flash, _app_ctx_stack, jsonify
from subprocess import call


app = Flask(__name__)


#global variables
NAMENODE_ADDRESS = '54.218.46.77:5000'
AWS_KEY_LOCATION = '/home/ec2-user/SUFS/venv/key/HaileysKey.pem'
DATANODE_STATIC_FILE_LOCATION = '/home/ec2-user/SUFS/venv/datanode/contents'
FROM_DATANODE_FILES_LOCATION = '/home/ec2-user/SUFS/venv/filesFromDatanodes'
BLOCK_SIZE = 32000000
TRIGGERED = False


# function that extracts key and name from S3 file path
def extractS3Info(S3Link):
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

	while bucketPosition != linkLength:
		S3Key += S3Link[bucketPosition] 
		bucketPosition += 1

	S3Info = {'S3Key' : S3Key, 'S3Bucket': S3Bucket}
	return S3Info

#this function extracts only file name from S3 key
def extractFileName(filePath):
	fileName = filePath.replace('/', '*')
	return fileName

def replaceStar(metadata):
	for key in metadata.keys():
		metadata[key.replace('*', '/')] = metadata.pop(key)
	return metadata

def extractNewFileName(fileName):
	index = len(fileName) - 1
	while  index != 0:
		if fileName[index] != '*':
			index -= 1
		else:
			break
	return fileName[index + 1:]

def splitFileInBlocks(newFilePath, fileName):
	fid = 1
	with open(newFilePath) as infile:
		def readOneBlock():
			return infile.read(BLOCK_SIZE) #test with different value 67108864
		for block in iter(readOneBlock, ''):
			newFileName = fileName + '_' +str(fid)
			f = open(newFileName, 'w')
			f.write(block)
			f.close()
			fid += 1

def combineBlocksInFile(fileName, blocks):
	newFilePath = FROM_DATANODE_FILES_LOCATION + '/' + extractNewFileName(fileName)
	with open(newFilePath, 'w') as infile:
		for block in blocks:
			blockPath = FROM_DATANODE_FILES_LOCATION + '/' + block
			f = open(blockPath, 'r')
			infile.write(f.read())
			f.close()
		infile.close()

def triggerFunctions():
	global TRIGGERED
	TRIGGERED = True
	triggerReportAddress = 'http://' + NAMENODE_ADDRESS + '/trigger_all_report'
	try:
		res = requests.get(triggerReportAddress)
	except requests.exceptions.RequestException as e:
		return 'error'

	triggerReplicaCheck = 'http://' + NAMENODE_ADDRESS + '/replica_check'
	try:
		res = requests.get(triggerReplicaCheck)
	except requests.exceptions.RequestException as e:
		return 'error'


@app.route('/')
def hello_world():
	if not TRIGGERED:
		triggerFunctions()
	return render_template('layout.html')

# accept S3 file path only for simplicity and efficiency 
@app.route('/create_file', methods=['GET', 'POST'])
def create_file():
	error = None
	if request.method == 'POST':
		# send file path to the data node
		filePath = request.form['filepath']
		S3Path = request.form['S3filepath']
		fileName = extractFileName(filePath)
		S3Info = extractS3Info(S3Path)

		BUCKET_NAME = S3Info['S3Bucket'] # replace with your bucket name
		KEY = S3Info['S3Key'] # replace with your object key

		#1. Download file from S3
		s3 = boto3.resource('s3')
		newFilePath = '/home/ec2-user/SUFS/venv/S3Download/downloadedFile' # + KEY

		try:
			s3.Bucket(BUCKET_NAME).download_file(KEY, newFilePath)
		except botocore.exceptions.ClientError as e:
			if e.response['Error']['Code'] == '404':
				return render_template('create_file.html', data={'valid': -1, 'status': 'File does not exist in S3'})
			else:
				return render_template('create_file.html', data={'valid': -1, 'status': 'S3 Bucket Doesnt exist'})

		S3FileSize = os.stat(newFilePath).st_size
		
		#2. contact to data node and check if filepath exists, gets num blocks if it doesn't exist
		dataToSend = {'filePath':fileName, 'fileSize': S3FileSize} # this path should be a path where client wants to save data in 
		namenodeCreateFilePath = 'http://' + NAMENODE_ADDRESS + '/create_file'
		
		try:
			res = requests.get(namenodeCreateFilePath, json=dataToSend) # contact namenode to get num of blocks and data node addressees
		except requests.exceptions.RequestException as e:
			return render_template('create_file.html', data={'valid': -1, 'status': 'Name node connection error'})
		namenodeData = res.json()
		

		# check if the file already exists or not
		isFileExists = namenodeData['valid']
		
		if isFileExists == -1:
			return render_template('create_file.html', data={'valid': -1, 'status': 'File Already Exists'})
		else:
			#3. Split the file into blocks
			splitFileInBlocks(newFilePath, fileName)

		#4. Store the block in Data nodes (contact ec2 with blocks)
		blockNums = len(namenodeData['addresses'])

		fid = 1
		for address in namenodeData['addresses']:
			fileToSend = fileName + '_' + str(fid)
			fileTransferCommand = 'scp -i ' + AWS_KEY_LOCATION + ' ' + fileToSend +' ec2-user@' + address + ':' + DATANODE_STATIC_FILE_LOCATION
			fid += 1
			try:
				call(fileTransferCommand.split())
			except requests.exceptions.RequestException as e:
				return render_template('create_file.html', data={'valid': -1, 'status': 'Data node error!'})
			

			datanodeCreateReplica= 'http://' + address + ':5000/make_replica'
			dataToSend = {'filename': fileToSend}

			try:
				res = requests.post(datanodeCreateReplica, json=dataToSend) # contact namenode to get num of blocks and data node addressees
			except requests.exceptions.RequestException as e:
				return render_template('create_file.html', data={'valid': -1, 'status': 'Replica failure'})

			#after transferring the file, remove from the client
			removeFileCommand = 'rm ' + fileToSend
			os.system(removeFileCommand)

			fileToSend = ''

		return render_template('create_file.html', data={'valid': 1, 'status': 'Stored Successfully!'})
	else:
		if not TRIGGERED:
			triggerFunctions()
		return render_template('create_file.html', error=error)


@app.route('/read_file', methods=['GET', 'POST'])
def read_file():
	error = None
	if request.method == 'POST':
		filePath = request.form['filepath']
		fileName = extractFileName(filePath)

		namenodeReadFilePath = 'http://' + NAMENODE_ADDRESS + '/read_file'
		dataToSend = {'filename': fileName}

		# contact namenode 
		try:
			res = requests.get(namenodeReadFilePath, json=dataToSend) # contact namenode to get num of blocks and data node addressees
		except requests.exceptions.RequestException as e:
			return render_template('create_file.html', data={'valid': -1, 'status': 'File Not Exists'})

		#contact datanodes and get files
		namenodeData = res.json()
		blocks = namenodeData['blocks']
		numKeys = len(blocks.keys())
		orderedIps = []

		# sort ip addresses in order
		for x in range(1, numKeys + 1):
			tempKey = '_' + str(x)
			orderedIps.append(blocks[tempKey]) 


		index = 0
		blockNum = 0
		blockNames = []
		for ips in orderedIps:
			blockNum += 1
			ip = ips[index]
			# send heartbeat
			ec2_address_heartbeat = 'http://' + ip + ':5000/heartbeat'
			if res.status_code == requests.codes.ok:
				# contact namenode 
				datanodeGetFileAddress = 'http://' + ip + ':5000/get_block'
				try:
					fileNameWithBlockNum = fileName + '_' + str(blockNum)
					blockNames.append(fileNameWithBlockNum)
					dataToSend = {'fileName': fileNameWithBlockNum}
					res = requests.get(datanodeGetFileAddress, json=dataToSend) # contact namenode to get num of blocks and data node addressees
					fileNameWithBlockNum = ''
				except requests.exceptions.RequestException as e:
					return render_template('read_file.html', data={'valid': -1, 'status': 'File Transfer Error'})

		# combile all files into one
		combineBlocksInFile(fileName, blockNames)

		# delete old files
		removeFileCommand = 'rm ' + FROM_DATANODE_FILES_LOCATION + '/' + fileName + '_*'
		os.system(removeFileCommand)

		return render_template('read_file.html', data={'status': 'File downloaded! go check your directory'})
	else:
		if not TRIGGERED:
			triggerFunctions()
		return render_template('read_file.html', error=error)

@app.route('/delete_file', methods=['GET', 'POST'])
def delete_file():
	error = None
	if request.method == 'POST':
		# send file path to the data node
		filePath = request.form['filepath']
		fileName = extractFileName(filePath)
		namenodeDeleteFilePath = 'http://' + NAMENODE_ADDRESS + '/read_file'
		dataToSend = {'filename': fileName}

		# contact namenode 
		try:
			res = requests.get(namenodeDeleteFilePath, json=dataToSend) # contact namenode to get num of blocks and data node addressees
		except requests.exceptions.RequestException as e:
			return render_template('delete_file.html', data={'valid': -1, 'status': 'File Not Exists'})

		#contact datanodes and get files
		namenodeData = res.json()
		blocks = namenodeData['blocks']

		blockNum = 0

		for block, values in blocks.items(): # block = [127.434.43.43, 127.4324.43.42, 127.3.4.2]
			fileNameWithBlockNum = fileName + str(block)
			for ip in values: #ip = 127.5.5.5
				ec2_address_heartbeat = 'http://' + ip + ':5000/heartbeat'
				if res.status_code == requests.codes.ok:
					# contact namenode 
					datanodeDeleteBlockAddress = 'http://' + ip + ':5000/delete_block'
					try:
						dataToSend = {'fileName': fileNameWithBlockNum}
						res = requests.delete(datanodeDeleteBlockAddress, json=dataToSend) # contact namenode to get num of blocks and data node addressees
					except requests.exceptions.RequestException as e:
						return render_template('delete_file.html', data={'status': 'File Delete Error'})
		return render_template('delete_file.html', data={'status': 'File Deleted!'})
	else:
		if not TRIGGERED:
			triggerFunctions()
		return render_template('delete_file.html', error=error)

@app.route('/create_directory', methods=['GET', 'POST'])
def create_directory():
	error = None
	if request.method == 'POST':
		directoryName = request.form['filepath'] # maybe change this later
		if directoryName[-1:] != '/':
			directoryName += '/'
		directoryName = extractFileName(directoryName)

		# get list contents

		dataToSend = {'directoryName':directoryName} 
		namenodeCreateDirectory = 'http://' + NAMENODE_ADDRESS + '/create_directory'
		
		try:
			res = requests.post(namenodeCreateDirectory, json=dataToSend) # contact namenode to get num of blocks and data node addressees
		except requests.exceptions.RequestException as e:
			return render_template('create_directory.html', data={'valid': -1, 'status': 'Name node connection error'})
		namenodeData = res.json()

		if namenodeData['valid'] == 0:
			return render_template('create_directory.html', data={'status': 'Directory created!'})
		else:
			return render_template('create_directory.html', data={'status': 'Directory already exists!'})
	else:
		if not TRIGGERED:
			triggerFunctions()
		return render_template('create_directory.html', error=error)

@app.route('/delete_directory', methods=['GET', 'POST'])
def delete_directory():
	error = None
	if request.method == 'POST':
		directoryName = request.form['filepath'] # maybe change this later

		if directoryName[-1:] != '/':
			directoryName += '/'

		directoryName = extractFileName(directoryName)
		dataToSend = {'directoryName':directoryName} 

		# list contents
		namenodeListContents = 'http://' + NAMENODE_ADDRESS + '/list_contents'
		
		try:
			res = requests.post(namenodeListContents, json=dataToSend) # contact namenode to get num of blocks and data node addressees
		except requests.exceptions.RequestException as e:
			return render_template('delete_dirrectory.html', data={'valid': -1, 'status': 'Name node connection error'})
		namenodeData = res.json()
		directoryContent = namenodeData['directoryContent']

		if len(directoryContent) == 0:
			return render_template('delete_directory.html', data={'status': 'Directory does not exist!'})

		if len(directoryContent) == 1: # means it is empty
			dataToSend = {'directoryName':directoryName} 
			namenodeDeleteDirectory = 'http://' + NAMENODE_ADDRESS + '/delete_directory'
			
			try:
				res = requests.post(namenodeDeleteDirectory, json=dataToSend) # contact namenode to get num of blocks and data node addressees
			except requests.exceptions.RequestException as e:
				return render_template('delete_directory.html', data={'valid': -1, 'status': 'Name node connection error'})
			namenodeData = res.json()


			if namenodeData['valid'] == 0:
				return render_template('delete_directory.html', data={'status': 'Directory deleted!'})
			else:
				return render_template('delete_directory.html', data={'status': 'Directory does not exist!'})
		else:
			return render_template('delete_directory.html', data={'status': 'Directory is not empty. Can not delete it'})
	else:
		if not TRIGGERED:
			triggerFunctions()
		return render_template('delete_directory.html', error=error)


@app.route('/list_contents', methods=['GET', 'POST'])
def list_contents():
	error = None
	if request.method == 'POST':
		directoryName = request.form['filepath'] 

		# check if / exists
		if directoryName[-1:] != '/':
			directoryName += '/'

		directoryName = extractFileName(directoryName)
		dataToSend = {'directoryName':directoryName} 
		namenodeListContents = 'http://' + NAMENODE_ADDRESS + '/list_contents'
		
		try:
			res = requests.post(namenodeListContents, json=dataToSend) # contact namenode to get num of blocks and data node addressees
		except requests.exceptions.RequestException as e:
			return render_template('list_contents.html', data={'valid': -1, 'status': 'Name node connection error'})
		namenodeData = res.json()

		if (len(namenodeData['directoryContent']) == 0):
			return render_template('list_contents.html', data={'status': 'No such directory'})

		directoryContent = namenodeData['directoryContent']
		return render_template('list_contents.html', data={'contents': directoryContent})
	else:
		if not TRIGGERED:
			triggerFunctions()
		return render_template('list_contents.html', data={})

@app.route('/list_datanodes', methods=['GET', 'POST'])
def list_datanodes():
	error = None
	if request.method == 'POST':
		filePath= request.form['filepath'] 

		listDatanodes = 'http://' + NAMENODE_ADDRESS + '/list_datanodes'
		
		try:
			res = requests.get(listDatanodes) # contact namenode to get num of blocks and data node addressees
			namenodeData = res.json()
			if namenodeData['valid'] == -1:
				return render_template('list_datanodes.html', data={'status': 'All namenodes are empty'})

			if extractFileName(filePath) not in namenodeData['datanodeContent']:
				return render_template('list_datanodes.html', data={'status': 'file does not exist'})

			replicas = namenodeData['datanodeContent'].pop(extractFileName(filePath))

			return render_template('list_datanodes.html', data={'valid': 1, 'contents': replaceStar(replicas)})

		except requests.exceptions.RequestException as e:
			return render_template('list_datanodes.html', data={'valid': -1, 'status': 'Name node connection error'})
	else:
		if not TRIGGERED:
			triggerFunctions()
		return render_template('list_datanodes.html', data={})

app.secret_key = 'A0Zr98j/3yX R~XHH!jmN]LWX/,?RT'


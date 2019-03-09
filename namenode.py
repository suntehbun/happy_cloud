import requests, json, math, threading
from flask import Flask, request, session, url_for, redirect, \
     render_template, abort, g, flash, _app_ctx_stack, jsonify
from random import randint

app = Flask(__name__)

# metadata data structure
metadata = {}

# global variables
# private ip
datanode_addresses = ['', '', '', '']
datanode_ip_map = {'a': '1', 'b': '2', 'c': '3', 'd': '4'}
datanode_nums = 4 
roundrobin_counter = 4
BLOCK_SIZE = 32000000
REPLICA_NUM = 2


def extractFileName(fileName):
	index = len(fileName) - 1
	while  index != 0:
		if fileName[index] != '_':
			index -= 1
		else:
			break

	return fileName[:index]


def extractBlockNum(fileName):
	index = len(fileName) - 1
	while  index != 0:
		if fileName[index] != '_':
			index -= 1
		else:
			break

	return fileName[index:]


@app.route('/')
def hello_world():
	return 'connected to the name node'

@app.route('/trigger_all_report', methods=['GET'])
def trigger_all_report():
	for address in datanode_addresses:
		heartbeatAddress = 'http://' + address +':5000/trigger_report'
		try:
			res = requests.get(heartbeatAddress)
		except requests.exceptions.RequestException as e:
			return 'error'

	return 'ok'

#get file paht and file size from the client and return the number of blocks and destination
@app.route('/create_file', methods=['GET'])
def receive_file():
	input_json = request.get_json(force=True)
	file_path = input_json.get('filePath')
	file_size = input_json.get('fileSize')

	if file_path in metadata:
		dataToReturn = {'valid': -1} # assume -1 indicates failure
	else:
		metadata[file_path] = {}
		num_blocks = math.ceil(file_size / BLOCK_SIZE)
		if file_size % BLOCK_SIZE > 0:
			num_blocks += 1
		blocks = 0
		ec2_addresses = []
		# pick ec2 instances who are available -> round robin approach
		global roundrobin_counter
		global datanode_addresses
		while blocks != num_blocks:
			ec2_address_index = roundrobin_counter % len(datanode_addresses)
			ec2_address_heartbeat = 'http://' + datanode_addresses[ec2_address_index] + ':5000/heartbeat'
			res = requests.get(ec2_address_heartbeat)
			if res.status_code == requests.codes.ok:
				ec2_addresses.append(datanode_addresses[ec2_address_index])
			roundrobin_counter += 1
			blocks += 1
		dataToReturn = {'valid': 0, 'addresses': ec2_addresses}
	return jsonify(dataToReturn)


@app.route('/receive_report', methods=['POST'])
def receive_report():
	input_json = request.get_json(force=True)
	files = input_json.get('files')
	deletedFiles = input_json.get('deletedFiles')

	publicIp = request.remote_addr
	privateIp = datanode_ip_map[publicIp]
	global metadata

	for file in files:
		key = extractFileName(file)
		if key not in metadata:
			metadata[key] = {}
		blockDictionary = metadata[key]
		blockKey = extractBlockNum(file)
		if blockKey not in blockDictionary:
			blockDictionary[blockKey] = []
		
		ipList = blockDictionary[blockKey]
		if privateIp not in ipList:
			ipList.append(privateIp)

	for file in deletedFiles:
		if file in metadata:
			del metadata[file]

	return 'ok'


@app.route('/read_file', methods=['GET'])
def read_file():
	input_json = request.get_json(force=True)
	fileName = input_json.get('filename')

	global metadata

	if fileName not in metadata:
		dataToReturn = {'valid': -1, 'error': 'file not exists'}
		return jsonify(dataToReturn)
	else:
		info = metadata[fileName]
		dataToReturn = {'valid': 0, 'blocks': info}
		return jsonify(dataToReturn)

@app.route('/create_directory', methods=['POST'])
def create_directory():
	input_json = request.get_json(force=True)
	directoryName = input_json.get('directoryName')

	if directoryName not in metadata:
		metadata[directoryName] = {}
		dataToReturn = {'valid': 0}
		return jsonify(dataToReturn)
	else:
		dataToReturn = {'valid': -1}
		return jsonify(dataToReturn)

@app.route('/delete_directory', methods=['POST'])
def delete_directory():
	input_json = request.get_json(force=True)
	directoryName = input_json.get('directoryName')

	if directoryName not in metadata:
		dataToReturn = {'valid': -1}
		return jsonify(dataToReturn)
	else:
		metadata.pop(directoryName)
		dataToReturn = {'valid': 0}
		return jsonify(dataToReturn)

@app.route('/list_contents', methods=['POST'])
def list_contents():
	input_json = request.get_json(force=True)
	directoryName = input_json.get('directoryName')

	directoryContent = set()
	dirLength = len(directoryName)

	for key in metadata.keys():
		if key[0:dirLength] == directoryName:
			secondary = key[dirLength:]
			splitSecondary = secondary.split('*')
			if len(splitSecondary) > 1:
				directoryContent.add(splitSecondary[0]+'/')
			else:
				directoryContent.add(splitSecondary[0])

	dataToReturn = {'valid': 0, 'directoryContent': list(directoryContent)}
	return jsonify(dataToReturn)

@app.route('/list_datanodes', methods=['GET'])
def list_datanodes():
	global metadata

	if not bool(metadata):
		dataToReturn = {'valid': -1}
		return jsonify(dataToReturn)

	dataToReturn = {'valid': 0, 'datanodeContent': metadata}
	return jsonify(dataToReturn)


@app.route('/replica_check', methods=['GET'])
def replica_check():

	for address in datanode_addresses:
		filePaths = []
		currentReplicas = []
		badIp = ''

		try:
			heartbeatAddress = 'http://' + address + ':5000/heartbeat'
			res = requests.get(heartbeatAddress) 

		except requests.exceptions.RequestException as e:
			badIp = address
			datanode_addresses.remove(badIp)
			for fileName, numberDic in metadata.items():
			    for numberBlock,ipList in numberDic.items():
			        if badIp in ipList:
			            ipList.pop(ipList.index(badIp))
			            filePaths.append(fileName+numberBlock) # file name _ num ex. please*work_5
			            currentReplicas.append(ipList)	#list of ip addresses that please*work_5 is still stored at
			
			if len(filePaths) > 0:
				for x in range(len(filePaths)):
					filename = filePaths[x]
					goodIp = currentReplicas[x][0]
					addressToSend = ''					
					while len(currentReplicas[x]) != REPLICA_NUM:
						addressToSend = datanode_addresses[randint(0, len(datanode_addresses) - 1)]
						if addressToSend not in currentReplicas[x]:
							currentReplicas[x].append(addressToSend)
					# send a request
					dataToSend = {'fileName':filename, 'destination': addressToSend, 'badIp': badIp} # this path should be a path where client wants to save data in 
					datanodeMakeReplica = 'http://' + goodIp+ ':5000/make_failed_replica'
					
					try:
						res = requests.post(datanodeMakeReplica, json=dataToSend) # contact namenode to get num of blocks and data node addressees
					except requests.exceptions.RequestException as e:
						return 'error on making replica'						

	threading.Timer(10, replica_check).start()

	return 'ok'


app.secret_key = ''


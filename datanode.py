import requests, json, time, threading, boto3, os, socket
from flask import Flask, request, session, url_for, redirect, \
     render_template, abort, g, flash, _app_ctx_stack, jsonify
from os import listdir
from os.path import isfile, join
from subprocess import call
from random import randint


app = Flask(__name__)

ec2 = boto3.resource('ec2')
instance = ec2.Instance('id')

#global variables
NAMENODE_ADDRESS = '5000'
NAMENODE_REPORT = 'receive_report'
CONTENTS_PATH = ''
CLIENT_STATIC_FILE_LOCATION = ''
AWS_KEY_LOCATION = ''
DATANODE_ADDRESSES = ['']
DATANODE_STATIC_FILE_LOCATION = ''
AWS_KEY_LOCATION = ''
DELETED_BLOCKS = set()
REPLICA_NUM = 2


def extractFileName(fileName):
	index = len(fileName) - 1
	while  index != 0:
		if fileName[index] != '_':
			index -= 1
		else:
			break

	return fileName[:index]


@app.route('/trigger_report', methods=['GET'])
def send_report():
	global DELETED_BLOCKS
	onlyfiles = [f for f in listdir(CONTENTS_PATH) if isfile(join(CONTENTS_PATH, f))]
	dataToSend = {'files': onlyfiles, 'deletedFiles': list(DELETED_BLOCKS)}
	try:
		res = requests.post(NAMENODE_REPORT, json=dataToSend) 
	except requests.exceptions.RequestException as e:
		pass
	DELETED_BLOCKS = set()
	threading.Timer(10, send_report).start()

	return 'ok'

@app.route('/get_block', methods=['GET'])
def get_block():
	input_json = request.get_json(force=True)
	fileName = input_json.get('fileName')

	fileTransferCommand = 'scp -i ' + AWS_KEY_LOCATION + ' ' + CONTENTS_PATH + '/' + fileName +' ec2-user@' + request.remote_addr + ':' + CLIENT_STATIC_FILE_LOCATION

	try:
		call(fileTransferCommand.split())
		return 'ok'
	except requests.exceptions.RequestException as e:
		dataToSend = {'valid': -1}
		return jsonify(dataToSend)


@app.route('/delete_block', methods=['DELETE'])
def delete_block():
	input_json = request.get_json(force=True)
	fileName = input_json.get('fileName')
	global DELETED_BLOCKS
	DELETED_BLOCKS.add(extractFileName(fileName))

	removeFileCommand = 'rm ' + CONTENTS_PATH + '/' + fileName 
	os.system(removeFileCommand)

	return 'ok'

@app.route('/make_replica', methods=['POST'])
def make_replica():
	input_json = request.get_json(force=True)
	fileName = input_json.get('filename')
	fileName = DATANODE_STATIC_FILE_LOCATION + '/' + fileName
	myIp = socket.gethostbyname(socket.getfqdn())
	chosenIp = []

	for replica in range(1, REPLICA_NUM):
		while len(chosenIp) != REPLICA_NUM - 1:
			randomIp = DATANODE_ADDRESSES[randint(0, len(DATANODE_ADDRESSES) -1)]
			if randomIp != myIp and randomIp not in chosenIp:
				# send heartbeat
				namenodeHeartbeat = 'http://' + randomIp + ':5000/heartbeat'
				try:
					res = requests.get(namenodeHeartbeat) # contact namenode to get num of blocks and data node addressees
					chosenIp.append(randomIp)
				except requests.exceptions.RequestException as e:
					DATANODE_ADDRESSES.remove(randomIp)
				
	for ip in chosenIp:
		fileTransferCommand = 'scp -i ' + AWS_KEY_LOCATION + ' ' + fileName +' ec2-user@' + ip + ':' + DATANODE_STATIC_FILE_LOCATION
		try:
			call(fileTransferCommand.split())
		except requests.exceptions.RequestException as e:
			return 0

	return 'connected to make replica'


@app.route('/make_failed_replica', methods=['POST'])
def make_failed_replica():
	input_json = request.get_json(force=True)
	fileName = input_json['fileName']
	destinationIp = input_json['destination']
	badIp = input_json['badIp']
	if badIp in DATANODE_ADDRESSES:
		DATANODE_ADDRESSES.remove(badIp)

	# send heartbeat
	namenodeHeartbeat = 'http://' + destinationIp + ':5000/heartbeat'
	
	try:
		res = requests.get(namenodeHeartbeat) # contact namenode to get num of blocks and data node addressees
	except requests.exceptions.RequestException as e:
		DATANODE_ADDRESSES.remove(destination)
		return 'error'

	fileTransferCommand = 'scp -i ' + AWS_KEY_LOCATION + ' ' + DATANODE_STATIC_FILE_LOCATION + '/' + fileName +' ec2-user@' + destinationIp + ':' + DATANODE_STATIC_FILE_LOCATION
	try:
		call(fileTransferCommand.split())
	except requests.exceptions.RequestException as e:
		return 'error sending replica'

	return 'ok'


@app.route('/')
def hello_world():
	return 'connected to the name node'

@app.route('/heartbeat', methods=['GET'])
def is_alive():
	return 'is_alive'




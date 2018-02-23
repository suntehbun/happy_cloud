import requests, json
from flask import Flask, request, session, url_for, redirect, \
     render_template, abort, g, flash, _app_ctx_stack, jsonify
from os import listdir
from os.path import isfile, join
import time, threading
import boto3


app = Flask(__name__)

ec2 = boto3.resource('ec2')
instance = ec2.Instance('id')

#global variables
NAMENODE_ADDRESS = '172.31.21.108'
CONTENTS_PATH = '/home/ec2-user/SUFS/venv/datanode/contents'


def getTMStatFilePaths():
    #will need root of qkview
    #finds all subdirs of snapshots and stores in file
    os.system('find '+CONTENTS_PATH+' -type d > output.txt')
    pathFile = open("output.txt", "r")
    pathList = pathFile.readlines()
    pathFile.close()
    os.system('rm output.txt')

def get_myIP():
	print('private ip address: ', ec2.Instance('private_ip_address'))
	return ec2.Instance('private_ip_address')

def send_report():
	# need list of files
	onlyfiles = [f for f in listdir(CONTENTS_PATH) if isfile(join(CONTENTS_PATH, f))]
	print('onlyfiles: ', onlyfiles)
	#convert to json
	dataToSend = {'address': get_myIP(), 'files': onlyfiles}
	# send to the namenode
	jsonify(dataToSend)
	try:
		res = requests.post(NAMENODE_ADDRESS+'/receive_report', json=dataToSend) 
	except requests.exceptions.RequestException as e:
		return 'block report failure'

	threading.Timer(10, send_report).start()



@app.route('/')
def hello_world():
	return 'connected to the name node'

@app.route('/heartbeat', methods=['GET'])
def is_alive():
	return 'is_alive'



# send block report to namenode in every 10 seconds
send_report()



import requests, json
import math
from flask import Flask, request, session, url_for, redirect, \
     render_template, abort, g, flash, _app_ctx_stack, jsonify


app = Flask(__name__)


# metadata data structure
metadata = {}

# global variables
datanode_addresses = ['172.31.25.233']
datanode_nums = 1 #chage this to 5 after testing with value 1
roundrobin_counter = 1


@app.route('/')
def hello_world():
	return 'connected to the name node'

#get file paht and file size from the client and return the number of blocks and destination
@app.route('/create_file', methods=['GET'])
def receive_file():
	input_json = request.get_json(force=True)
	print ('data from the client: ', input_json)
	file_path = input_json.get('filePath')
	file_size = input_json.get('fileSize')

	if file_path in metadata:
		dataToReturn = {'valid': -1} # assume -1 indicates failure
	else:
		metadata[file_path] = []
		num_blocks = math.ceil(file_size / 67108864)
		if file_size % 67108864 > 0:
			num_blocks += 1
		blocks = 0
		ec2_addresses = []
		print('num_blocks: ', num_blocks)
		# pick ec2 instances who are available -> round robin approach
		global roundrobin_counter
		global datanode_addresses
		while blocks != num_blocks:
			ec2_address_index = roundrobin_counter % datanode_nums
			print('ec2 address index: ', ec2_address_index)
			ec2_address_heartbeat = 'http://' + datanode_addresses[ec2_address_index] + ':5000/heartbeat'
			print('ec2_address_heartbeat: ', ec2_address_heartbeat)
			res = requests.get(ec2_address_heartbeat)
			if res.status_code == requests.codes.ok:
				ec2_addresses.append(datanode_addresses[ec2_address_index])
			roundrobin_counter += 1
			blocks += 1
			print('blocks: ', blocks)
			print('num_blocks: ', num_blocks)
		print('while loop broken')
		dataToReturn = {'valid': 0, 'addresses': ec2_addresses}
	return jsonify(dataToReturn)
			


app.secret_key = 'A0Zr98j/3yX R~XHH!jmN]LWX/,?RT'


import requests, json
from flask import Flask, request, session, url_for, redirect, \
     render_template, abort, g, flash, _app_ctx_stack, jsonify

app = Flask(__name__)

@app.route('/')
def hello_world():
	return 'connected to the name node'

@app.route('/heartbeat', methods=['GET'])
def is_alive():
	return 'is_alive'


from flask import Flask, request,Response, make_response
from cassandra.cluster import Cluster
import os
from flask_restful import Resource, Api, reqparse

cluster = Cluster()
session = cluster.connect(keyspace='hw5')

app = Flask(__name__)
@app.route("/")
def hello():
        return "Hello, I love Digital OOO!"

@app.route("/deposit", methods=['POST'])
def deposit():
        if request.method == 'POST':
                print('===================DEPOSIT===================')
                file = request.files.get('contents')
                filename = request.form.get('filename')
                b = bytearray(file.read())
                cluster = Cluster()
		session = cluster.connect(keyspace='hw5')

                cqlinsert = "INSERT INTO hw5.imgs(filename, contents) VALUES (%s, %s);"
                session.execute(cqlinsert, (filename, b))
                return 'OK'

@app.route("/retrieve", methods=['POST', 'GET'])
def retrieve():
        if request.method == 'GET':
                print('===================RETRIEVE===================')
                cluster = Cluster()
		session = cluster.connect(keyspace='hw5')
                args = parse_args_list(['filename'])
                query = "SELECT * FROM hw5.imgs WHERE filename = '" + args['filename'] + "';"
                row = session.execute(query)[0]
                filename = row[0]
                file = row[1]
                ending = filename.split('.')[1]
                response = make_response(file)
                response.headers.set('Content-Type', 'image/' + ending)
                return response
                
def parse_args_list(argnames):
	parser = reqparse.RequestParser()
	for arg in argnames:
		parser.add_argument(arg)
	args = parser.parse_args()
	return args

if __name__ == "__main__":
        app.run()

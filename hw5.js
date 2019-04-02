const util = require('util');
const express = require('express');
const app = express();
const port = 3000;

const formidable = require('formidable');

const cassandra = require('cassandra-driver');

app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(express.static(__dirname + "/static"));


const client = new cassandra.Client({
    contactPoints: ['127.0.0.1:9042'],
    localDataCenter: 'datacenter1',
    keyspace: 'hw5'
});

client.connect(function(err) {
    if (err) {
        console.log("Error connecting: " + err);
        return;
    }
    console.log("Connected to cluster.");
});

app.post('/deposit', function(req, res) {

    var query = "INSERT INTO imgs (filename, content) VALUES (?, ?)";

    var form = new formidable.IncomingForm();
    var chunks = [];

    form.onPart = function(part) {
        if (!part.filename) {
            form.handlePart(part);
            return;
        }
        part.on('data', function(data) {
            chunks.push(data);
            // console.log(data);
            // console.log("DATA: " + util.inspect(data, {showHidden: false, depth: null}));
        });

    res.json({status: "OK"});

    // res.json({status: "OK"});
    // return;
/*
    var filename = req.body.filename;
    var file = req.body.contents;
    console.log("filename: " + filename);
    console.log("file: " + file);
    */

});

app.get('/retrieve', function(req, res) {
    var filename = req.query.filename;
    console.log("filename: " + filename);

    var query = "SELECT filename, content FROM imgs WHERE filename = ?";
    client.execute(query, [filename], {prepare: true})
    .then(function(result) {
        var row = result.first();
        res.type(row['filename']);
        res.send(row['content']);
    })
    .catch(function(error) {
        console.log("Error retrieving file: " + error);
        res.json({status: "OK"});
    });

});

app.get('/Configure', function(req, res) {
    var createKeyspaceQuery = "CREATE KEYSPACE IF NOT EXISTS hw5 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1' }";
    var createTableQuery = "CREATE TABLE IF NOT EXISTS hw5.imgs (filename text, contents blob, PRIMARY KEY(filename))";

    client.execute(createKeyspaceQuery)
    .then(function(result) {
        console.log("Created keyspace: " + result);
        return client.execute(createTableQuery);
    })
    .then(function(result) {
        console.log("Created table: " + result);
    })
    .catch(function(error) {
        console.log("Error configuring cassandra: " + error);
    });
    res.json({status: "OK"});
});

app.listen(port, function() {
});

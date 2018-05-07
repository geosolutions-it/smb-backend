'use strict';

const AWS = require('aws-sdk');
const S3 = new AWS.S3();
const { Client } = require('pg');
var StreamZip = require('node-stream-zip');
const fs = require('fs');
const path = require('path');

//console.log('Loading function');

function gowiththeunzip(filename, callback) {
    
    var zip = new StreamZip({
        file: filename
        , storeEntries: true
        });

    zip.on('error', function (err) { console.error('[ERROR]', err); });
    
    zip.on('ready', function () {
      console.log('All entries read: ' + zip.entriesCount);
      //console.log(zip.entries());
    });
    
    zip.on('entry', function (entry) {
      var pathname = path.resolve('./temp', entry.name);
      if (/\.\./.test(path.relative('./temp', pathname))) {
          console.warn("[zip warn]: ignoring maliciously crafted paths in zip file:", entry.name);
          return;
      }
    
      if ('/' === entry.name[entry.name.length - 1]) {
        console.log('[DIR]', entry.name);
        return;
      }
    
      console.log('[FILE]', entry.name);
      
      zip.extract(entry.name, '/tmp/uncompressed.txt', err => {
        console.log(err ? 'Extract error' : 'Extracted');
        zip.close();
        readFile('/tmp/uncompressed.txt', fs.readFileSync('/tmp/uncompressed.txt', {encoding : 'utf-8'}), callback);
    });
      
      /*
      zip.stream(entry.name, function (err, stream) {
        if (err) { console.error('Error:', err.toString()); return; }
    
        stream.on('error', function (err) { console.log('[ERROR]', err); return; });

        stream.pipe(fs.createWriteStream('/tmp/uncompressed.txt'));

        readFile('/tmp/uncompressed.txt', fs.readFileSync('/tmp/uncompressed.txt', {encoding : 'utf-8'}), callback);
      });
      */
    });
}


function readFile(name, file, callback) {
    console.log("Reading "+ name);
    var lines = file.split('\n');
    var headers, values, insertquery, columnsString;
    var fieldsCount;
    
    const client = new Client();
    //console.log("Connecting to DB")
	client.on('notice', (msg) => console.warn('notice:', msg));
    client.on('error', (err) => {
        console.error('something bad has happened!', err.stack);
    });
    
    client.connect()
    .then(() => {
        console.log('connected');
        
        let queries = new Set();
        for (var i in lines) {
            // console.log(lines[i]);
            if(i==0){
                // Get the headers
                headers = lines[0].split(',');
                fieldsCount = headers.length;
                columnsString = "";
                for( var h in headers){
                    //console.log("Header: ->"+headers[h]+"<-");
                    if (headers[h] == '') continue;
                    if (h>0) columnsString += ',';
                    columnsString += headers[h];
                }
            } else {
                values = lines[i].split(',');
                if (values.length != fieldsCount) continue;
                
                insertquery = "INSERT INTO datapoints (" + columnsString + ") VALUES (";
                for( var v in values){
                    //console.log("Value: ->"+values[v]+"<-");
                    if (values[v] == '') continue;
                    if (v>0) insertquery += ',';
                    insertquery += values[v];
                }
                insertquery += ");";
               
                console.log("DB: "+insertquery);
                //const res = await client.query(insertquery);
                //console.log(res.rows?res.rows[0].message:res);
                
            	queries.add(client.query(insertquery));
            }
        }
        
        Promise.all(queries).then(value => { 
            console.log(value.length+" queries run");
            client.end();
            callback(null, null);
        }, reason => {
            console.log("Reason:");
            console.log(reason);
            callback(reason);
        });
    })
    .catch(e => {
        console.error('connection error', e.stack);
        callback(e);
    });

}


exports.handler = (event, context, callback) => {
    //console.log('Received event:', JSON.stringify(event, null, 2));
    const message = event.Records[0].Sns.Message;
    //console.log('From SNS:', message);
    var msg;
    try {
        msg = JSON.parse(message);
    } catch(e) {
        callback(e, message);
    }
    
    if (msg && msg.Records){
        console.log('RECORDS: '+ msg.Records.length);
        /*
        console.log('****  WILL NOT SIGNAL SNS BECAUSE I AM IN VPC  ****');
        
        const sns = new AWS.SNS();

        sns.publish({
            Message: 'Found '+msg.Records.length+ ' records' ,
            TopicArn: 'arn:aws:sns:eu-west-1:227022658256:lambda_match_found'
        }, function(err, data) {
            if (err) {
                console.log(err.stack);
                return;
            }
            console.log('push sent');
            console.log(data);
        });
        */
        
        
        // Retrieve the bucket & key for the uploaded S3 object that
        // caused this Lambda function to be triggered
        var src_bkt = msg.Records[0].s3.bucket.name;
        var src_key = msg.Records[0].s3.object.key;
    
        // Retrieve the object
        console.log("Retrieving S3 object");
/*
        console.log("Bucket: "+src_bkt);
        console.log("Key: "+src_key);
*/
        
        S3.getObject({
            Bucket: src_bkt,
            Key: src_key
        }, function(err, data) {
            if (err) {
                console.log(err, err.stack);
                callback(err);
            } else {
                console.log("Body length:\n" + data.Body.length);
                
                if(src_key.endsWith("zip")) {
                    
                    console.log("Got a ZIP file");
                    
                    fs.writeFile('/tmp/zipped.zip', data.Body, (err) => {
                      if (err) throw err;
                      console.log('The file has been saved!');
                      gowiththeunzip('/tmp/zipped.zip', callback);
                      
                    });
                    /*
                    zlib.unzip(data.Body, (err, buffer) => {
                      if (!err) {
                        console.log(buffer.toString());
                        readFile(src_key, buffer.toString('utf-8'), callback);
                      } else {
                          
                        console.log("ERROR UNZIPPING THE RECEIVED DATA");
                        console.error(err, err.stack);
                        // handle error
                      }
                    });
                    */
                } else {
                
                    readFile(src_key, data.Body.toString('utf-8'), callback);
                }
                
                //callback(null, null);
            }
        });
    }

    //callback(null, message);
};

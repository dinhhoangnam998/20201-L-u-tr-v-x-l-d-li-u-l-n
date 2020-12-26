#!/usr/bin/env node
var WebSocketServer = require('websocket').server;
var http = require('http');
const kafka = require('kafka-node');

// http server
var server = http.createServer(function (request, response) {
    console.log((new Date()) + ' Received request for ' + request.url);
    response.writeHead(404);
    response.end();
});

server.listen(7999, function () {
    console.log((new Date()) + ' Server is listening on port 7999');
});

// ws server
wsServer = new WebSocketServer({
    httpServer: server,
    autoAcceptConnections: false
});

function originIsAllowed(origin) {
    return true;
}

// kafka consumer
const Consumer = kafka.Consumer;
const client = new kafka.KafkaClient({ idleConnection: 24 * 60 * 60 * 1000, kafkaHost: "192.168.1.2:9092" });
let consumer = new Consumer(
    client,
    [{ topic: "predict_output", partition: 0 }],
    {
        autoCommit: true,
        fetchMaxWaitMs: 1000,
        fetchMaxBytes: 1024 * 1024,
        encoding: 'utf8',
        // fromOffset: false
    }
);

consumer.on('error', function (error) {
    console.log('error', error);
});


wsServer.on('request', function (request) {
    var connection = request.accept('echo-protocol', request.origin);
    console.log((new Date()) + ' Connection accepted.');

    // onKafkaMessage(connection)
    consumer.on('message', async function (message) {
        console.log(
            'kafka ',
            JSON.parse(message.value)
        );
        connection.sendUTF((message.value))
    })

    connection.on('close', function (reasonCode, description) {
        console.log((new Date()) + ' Peer ' + connection.remoteAddress + ' disconnected.');
    });
});

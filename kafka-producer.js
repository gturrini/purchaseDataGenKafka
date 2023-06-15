const { Kafka } = require('kafkajs');

var dataGen = require("./generateDataPurchase.js");

//const kafkaClientId = process.env.KAFKA_CLIENT_ID;
const kafkaClientId = 'yadda-3';
//const kafkaBrokersTmp = process.env.KAFKA_BROKERS;
const kafkaBrokersTmp = 'localhost:9092';
const kafkaBrokers = [`${kafkaBrokersTmp}`];


init();

let keepRunning = false;
let statusRunning = "KAFKA PRODUCER STOPPED: TRUE";

var express = require("express");
var app = express();
app.get("/start", (req, res, next) => {
    keepRunning = true;
    startProducer();
    statusRunning = "KAFKA PRODUCER START: TRUE";
    res.json(statusRunning);
});
app.get("/stop", (req, res, next) => {
    keepRunning = false;
    statusRunning = "KAFKA PRODUCER STOPPED: TRUE";
    res.json(statusRunning);
});
app.get("/status", (req, res, next) => {
    if (statusRunning == "KAFKA PRODUCER START: TRUE") { statusResponse = "KAFKA PRODUCER STATUS: RUNNING"; }
    else {
        if (statusRunning == "KAFKA PRODUCER STOPPED: TRUE") { statusResponse = "KAFKA PRODUCER STATUS: STOPPED"; }
        else {
            statusResponse = "KAFKA PRODUCER STATUS: UNKNOWN";
        }
    }
    res.json(statusResponse);
});

app.listen(3000, () => {
    console.log("Server running on port 3000");
});

function init() {
    if ((typeof kafkaClientId === 'undefined') || (typeof kafkaBrokers === 'undefined')) {
        const error = new Error('INVALID/MISSING Kafka parameters.');
        error.code = 400; // Set the error code
        throw error;
    }
    kafkaTopic = process.env.KAFKA_TOPIC;
    if (typeof kafkaTopic === 'undefined') { kafkaTopic = 'topicNotConfigured' }

    autoStart = process.env.AUTO_START;
    if (typeof autoStart === 'undefined') { autoStart = false }
    switch (autoStart) {
        case "1":
        case "Y":
        case "YES":
        case "y":
        case "yes":
        case "T":
        case "TRUE":
        case "t":
        case "true":
            autoStart = true;
        case "0":
        case "N":
        case "NO":
        case "n":
        case "no":
        case "F":
        case "FALSE":
        case "f":
        case "falsee":
            autoStart = false;
        default:
            autoStart = false;
    }
}
const partitionName =['Partition_01','Partition_02','Partition_03','Partition_04','Partition_05','Partition_06','Partition_07','Partition_08','Partition_09','Partition_10']

const kafka = new Kafka({
    //clientId: 'yadda-1',
    clientId: kafkaClientId,
    //brokers: ['localhost:9092'],
    brokers: kafkaBrokers,
});

async function sendEvent(message) {
    const producer = kafka.producer();
    try {
        await producer.connect();
        await producer.send({
            topic: kafkaTopic,
            messages: [{ value: message }],
        });
        console.log('Event sent successfully');
    } catch (error) {
        console.error('Error sending event:', error);
    } finally {
        await producer.disconnect();
    }
}

async function sendEventBatch(messageBatch) {
    const producer = kafka.producer();
    try {
        await producer.connect();
        for (let i = 0; i < messageBatch.length; i++) {
            await producer.send({
                topic: kafkaTopic,
                messages: [{ value: JSON.stringify(messageBatch[i]) }],
                partition: partitionName[Math.floor(Math.random() * 10)],
            });
            console.log('Event sent successfully');
        }
    } catch (error) {
        console.error('Error sending event:', error);
    } finally {
        await producer.disconnect();
    }
}

async function startProducer() {
    while (true) {
        if (keepRunning === false) { break; }
        //await sendEvent(JSON.stringify(dataGen.generatePurchase()));
        await sendEventBatch(dataGen.generatePurchaseBatch());
    }
}

if (autoStart) {
    keepRunning = true;
    startProducer();
}

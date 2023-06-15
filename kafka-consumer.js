const { Kafka } = require('kafkajs');

//const kafkaClientId = process.env.KAFKA_CLIENT_ID;
const kafkaClientId = 'yadda-3';
//const kafkaBrokers = process.env.KAFKA_BROKERS;
const kafkaBrokersTmp = 'localhost:9092';
const kafkaBrokers = [`${kafkaBrokersTmp}`];
//const kafkaConsumerGroup = process.env.KAFKA_CONSUMER_GROUP;
const kafkaConsumerGroup = 'pappappero-3';

init();

let keepRunning = false;
let statusRunning = "FALSE";

function init() {
    if ((typeof kafkaClientId === 'undefined') || (typeof kafkaBrokers === 'undefined')|| (typeof kafkaConsumerGroup === 'undefined')) {
        const error = new Error('INVALID/MISSING Kafka parameters.');
        error.code = 400; // Set the error code
        throw error;
    }
    kafkaTopic = process.env.KAFKA_TOPIC;
    if (typeof kafkaTopic === 'undefined') { kafkaTopic = 'topicNotConfigured' }

}

const kafka = new Kafka({
    //clientId: 'yadda-1',
    clientId: kafkaClientId,
    //brokers: ['localhost:9092'],
    brokers: kafkaBrokers,
});

async function consumeMessages(topic) {
    const consumer = kafka.consumer({ groupId: kafkaConsumerGroup });
  
    await consumer.connect();
    await consumer.subscribe({ topic });
  
    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        console.log(`Received message on topic "${topic}", partition ${partition}: ${message.value.toString()}`);
      },
    });
  }
  
  consumeMessages(kafkaTopic).catch((error) => {
    console.error('Error consuming messages:', error);
  });
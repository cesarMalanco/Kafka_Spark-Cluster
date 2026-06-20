const { Kafka } = require('kafkajs');
const fs = require('fs');

const kafka = new Kafka({
  clientId: 'consumer-node',
  brokers: ['192.168.1.101:9092', '192.168.1.102:9092', '192.168.1.103:9092']
});

const consumer = kafka.consumer({groupId: 'group1'});

// Output files
const fileCSV = 'data_node2.csv';
const fileSQL = 'data_node2.sql';
const fileJSON = 'data_node2.json';

async function run(){
  await consumer.connect();
  console.log("Consumer connected to the cluster");

  const topics = ['personas', 'ventas', 'logs-sistema', 'sensores', 'transacciones'];
  for(const t of topics){
    await consumer.subscribe({topic: t, fromBeginning: true});
  }

  let localCounter = 0;

  await consumer.run({
    eachMessage: async({topic, partition, message}) => {
      const payload = JSON.parse(message.value.toString());

      fs.appendFileSync(fileCSV, payload.csv + '\n');
      fs.appendFileSync(fileSQL, payload.sql + '\n');
      fs.appendFileSync(fileJSON, payload.json + '\n');

      localCounter++;
      console.log(`[OK] Record number ${localCounter} processed and saved successfully (Topic: ${topic})`);
      if(localCounter % 1000 === 0){
        console.log(`I have processed and saved ${localCounter} records on this node...`);
      }
    },
  });
}

process.on('SIGINT', async () => {
  console.log("\nSafely disconnecting the consumer...");
  await consumer.disconnect();
  process.exit(0);
});

run().catch(console.error);
const { Kafka } = require("kafkajs");
const { Client } = require("@elastic/elasticsearch");

async function kafkaToElasticsearch() {
  const kafka = new Kafka({
    clientId: "kafka-elasticsearch-bridge",
    brokers: ["kafka:9092"],
  });

  const consumer = kafka.consumer({ groupId: "elasticsearch-consumer" });
  const elasticClient = new Client({ node: "http://elasticsearch:9200" });

  try {
    await consumer.connect();
    console.log("Connected to Kafka");

    await consumer.subscribe({ topic: "chat-messages", fromBeginning: true });

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        console.log("Message received from Kafka: ", message.value.toString());
        try {
          const parsedMessage = JSON.parse(message.value.toString());
          await elasticClient.index({
            index: "chat-messages",
            body: {
              roomID: parsedMessage.roomID,
              sender: parsedMessage.message.sender,
              content: parsedMessage.message.content,
              type: parsedMessage.message.type,
              timestamp: parsedMessage.message.timestamp,
            },
          });
          console.log("Message indexed in Elasticsearch");
        } catch (error) {
          console.error("Error indexing message in Elasticsearch:", error);
        }
      },
    });

    console.log("Kafka consumer started");
  } catch (error) {
    console.error("Error in kafkaToElasticsearch function:", error);
  }
}

module.exports = kafkaToElasticsearch;

const { Kafka } = require("kafkajs");
const redis = require("redis");

async function redisToKafka() {
  const kafka = new Kafka({
    clientId: "redis-kafka-bridge",
    brokers: ["kafka:9092"],
  });

  const producer = kafka.producer();

  try {
    await producer.connect();
    console.log("Connected to Kafka");

    const redisClient = redis.createClient({
      url: "redis://redis:6379",
    });

    redisClient.on("error", (err) => console.log("Redis Client Error", err));

    await redisClient.connect();
    console.log("Connected to Redis");

    const subscriber = redisClient.duplicate();
    await subscriber.connect();

    await subscriber.subscribe("kafkaChannel", async (message) => {
      console.log("Message received from Redis: ", message);
      try {
        await producer.send({
          topic: "chat-messages",
          messages: [{ value: message }],
        });
        console.log("Message sent to Kafka");
      } catch (error) {
        console.error("Error sending message to Kafka:", error);
      }
    });

    console.log("Subscribed to kafkaChannel");
  } catch (error) {
    console.error("Error in redisToKafka function:", error);
  }
}

module.exports = redisToKafka;

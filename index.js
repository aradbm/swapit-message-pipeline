const redisToKafka = require("./redisToKafka");
const kafkaToElasticsearch = require("./kafkaToElasticsearch");

async function main() {
  await Promise.all([redisToKafka(), kafkaToElasticsearch()]);
}

main().catch(console.error);

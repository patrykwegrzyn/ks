const { Kafka } = require('kafkajs')

const { Consumer } = require('./kafka')

const Kstream = require('./kstream')

class StreamFactory {

  constructor(options) {
    this.broker = new Kafka(options.kafka)
  }

  async initialize(config) {
    const initializers = config.map( cfg => {
      const { topic, kind } = cfg;
      const consumer = this.createConsumer(cfg)
      if(kind === 'stream') {
        return new Kstream({topic, consumer}).initialize()
      }
    })
    return Promise.all(initializers)
  }

   createConsumer({ topic, groupId}) {
    console.log(topic, groupId)
    const consumer = new Consumer(this.broker, { groupId })
    //await consumer.start({topic})
    return consumer;
  }

}

module.exports = StreamFactory;

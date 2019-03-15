const { Kafka } = require('kafkajs')

const { Consumer } = require('./kafka')

const Kstream = require('./kstream')

class StreamFactory {

  constructor(options) {
    this.options = options;
    
    this.broker = new Kafka(options.kafka)
    this.admin  = this.broker.admin();
    this.consumers = []
  }

  async initialize(config) {
    await this.admin.connect();
    const initializers = config.map( async cfg => {
      const { topic, kind } = cfg;
      const consumer = await this.createConsumer(cfg)
      if(kind === 'stream') {
        return new Kstream({topic, consumer}).initialize()
      }
    })
    return Promise.all(initializers)
  }

   async createConsumer({topic}) {
    const { clientId } = this.options.kafka
    const groupId = `${clientId}:${topic}`;
    const consumer = new Consumer(this.broker, { groupId })
    this.consumers.push(consumer)
    console.log('this.comsumers', this.comsumers)
  
    //await consumer.start({topic})
    return consumer;
  }

  async disconnect( ) {
    console.log('trying to disconnect')
    const consumers = this.consumers.map( c => {
      return  c.consumer.disconnect()
    })
    console.log(consumers)
    await this.broker.disconnect()
    return Promise.all(consumers)
  }

}

module.exports = StreamFactory;

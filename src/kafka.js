const Events = require('events');

class Consumer extends Events {
  constructor(kafka, options) {
    super()
    this.consumer = kafka.consumer(options)

    const { DISCONNECT } = this.consumer.events
    const removeListener = this.consumer.on(DISCONNECT, e => console.log(`DISCONNECT at ${e.timestamp}`))

  }

  async eachMessage({ topic, partition, message }) {
    const { offset, key, value, timestamp } = message;
    this.emit('message', {
      topic,
      partition,
      offset,
      key: key ? key.toString() : key,
      value: value.toString(),
      timestamp
    })
  }

  async start({topic}) {
    console.log('start', topic)
    await this.consumer.connect()
    await this.consumer.subscribe({topic, fromBeginning: true })
    await this.consumer.run({
      eachMessage: this.eachMessage.bind(this)
    })
    await this.consumer.seek({ topic : topic, partition: 0 , offset: -2})
    return this;
  }
}

exports.Consumer = Consumer;
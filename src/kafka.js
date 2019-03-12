const Events = require('events');

class Consumer extends Events {
  constructor(kafka, options) {
    super()
    this.consumer = kafka.consumer(options)
  }

  async eachMessage({ topic, partition, message }) {
    const { offset, key, value, timestamp } = message;
    console.log('here')
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
    await this.consumer.subscribe({topic})
    await this.consumer.run({
      eachMessage: this.eachMessage.bind(this)
    })
    await this.consumer.seek({ topic : topic, partition: 0, offset: -2 })
    return this;
  }
}

exports.Consumer = Consumer;
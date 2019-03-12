
const Codec = require('./codec')
const { fromEvent } = require('most')

class KStream {

  constructor({ topic, consumer, codec }) {
    this.topic = topic;
    this.consumer = consumer;
    this.codec = codec || new Codec()
  }

  async decode(message) {
    message.value = await this.codec.decode(message.value)
    return message;
  }
  
  async initialize() {
    await this.consumer.start({topic: this.topic});
    this.stream = fromEvent('message',this.consumer)
    .map(this.decode.bind(this))
    .await()
          
    return this.stream
  }

}

module.exports = KStream;

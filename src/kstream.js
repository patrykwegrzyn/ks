const _ = require('highland');
const Codec = require('./codec')

class KStream {

  constructor({ topic, consumer, codec }) {
    this.topic = topic;
    this.consumer = consumer;
    this.codec = codec || new Codec()
  }

  initialize() {
    const decode = _.wrapAsync(this.codec.decode)
    this.stream = _(this.consumer).map(decode);
    return Promise.resolve(this);
  }

}

const methods = ['each', 'fork', 'resume', 'filter', 'group', 'map', 'batch'];

methods.forEach(m => {
  KStream.prototype[m] = function (eff) {
    this.stream = this.stream[m](eff);
    return this;
  };
});

module.exports = KStream;

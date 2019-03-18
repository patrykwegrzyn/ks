const events = require('events')
// const options = {
//   kafka: {
//     clientId: 'my-app-3',
//     brokers: ['xx.xx.xx.239:29092'],
//     requestTimeout: 5000
//   } 
// };

// const conf = [
// {
//   kind:'stream',
//   topic: 'public.device',
// },
// {
//   kind:'stream',
//   topic: 'public.gateway',
// }
// ]

const emitter = new events();


const { Kafka } = require('kafkajs')

// const client = new Kafka(options.kafka)
// const consumer = client.consumer({ groupId: 'test-group' })

// async function test() {
//   await consumer.connect()

//   await Promise.all(conf.map( async c => {
//     return consumer.subscribe({ topic:c.topic , fromBeginning: true})
//   }))

//   await consumer.run({
//     eachMessage: async ({topic, partition, message}) => {
//       const {timestamp, offset, key, value} = message;
//       emitter.emit('data', {
//         topic,
//         timestamp,
//         offset,
//         key: key.toString(),
//         value: value.toString()
//       });
//     },
//   })

//   await Promise.all( conf.map( ({ topic }) => {
//     return consumer.seek({ topic, partition: 0 , offset: -2})
//   }))

// }


// test()

// emitter.on('data', (data) => {
//   console.log('message', data)
// })

const most = require('most')

const valueDecoder = ( message ) => {
  let { value } = message;
  value = JSON.parse(value)
  return  {...message, value}
}


const select = topic => (message) => message.topic === topic
const main = most.fromEvent('data', emitter).multicast()
  

const gateway = main
  .filter(select('public.gateway'))
  .map(valueDecoder)
  

const device = main
  .filter(select('public.device'))
  .map(valueDecoder)

const merged = most.merge(gateway, device)
.forEach(event => console.log('merged',event))


class KStream {
  constructor(stream) {
    this.stream = stream;
  }

  select(topic) {
    const stream = this.stream.filter(m => m.topic === topic)
    return new KStream(stream);
  }

  multicast() {
    this.stream.multicast()
    return this
  }

  forEach(message) {
    this.stream = this.stream.forEach(message)
    return this;
  }
}

class KS extends events {
  constructor(options) {
    super()
    this.options = options;
    this.client = new Kafka(options.kafka);
 
  }

  async initialize(conf) {
    console.log(conf)

    const consumer = this.consumer = this.client.consumer({ 
      groupId: 'test-group' 
    })

    await consumer.connect()

    await Promise.all(conf.map(async c => {
      return consumer.subscribe({ 
        topic: c.topic,
        fromBeginning: true 
      })
    }))

    console.log('here')
    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        const { timestamp, offset, key, value } = message;
      //  console.log(topic, partition, message)
        this.emit('data', {
          topic,
          timestamp,
          offset,
          key: key ? key.toString(): key,
          value: value.toString()
        });
      },
    })
   

    console.log('here 2')

    await Promise.all(conf.map(({ topic }) => {
      return consumer.seek({ topic, partition: 0, offset: -2 })
    }))

    console.log('here 3')
  
    return new KStream(most.fromEvent('data', this))
  }

}




const options = {
  kafka: {
    clientId: 'my-app-3',
    brokers: ['localhost:29092'],
    requestTimeout: 5000
  }
};

const conf = [
  {
    kind: 'stream',
    topic: 'sensorType',
  },
  {
    kind: 'stream',
    topic: 'sensorAssets',
  }
]


const ks = new KS(options)


async function test() {
  const stream  = (await ks.initialize(conf)).multicast()

  stream
    .select('sensorAssets')
    .forEach(event => console.log('sensorAssets', event))
  stream
    .select('sensorType')
    .forEach(event => console.log('sensorType', event))
}

test()
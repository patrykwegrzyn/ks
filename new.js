const events = require('events')
const options = {
  kafka: {
    clientId: 'my-app-3',
    brokers: ['xx.xx.xx.239:29092'],
    requestTimeout: 5000
  } 
};

const conf = [
{
  kind:'stream',
  topic: 'public.device',
},
{
  kind:'stream',
  topic: 'public.gateway',
}
]

const emitter = new events();


const { Kafka } = require('kafkajs')

const client = new Kafka(options.kafka)
const consumer = client.consumer({ groupId: 'test-group' })

async function test() {
  await consumer.connect()

  await Promise.all(conf.map( async c => {
    return consumer.subscribe({ topic:c.topic , fromBeginning: true})
  }))

  await consumer.run({
    eachMessage: async ({topic, partition, message}) => {
      const {timestamp, offset, key, value} = message;
      emitter.emit('data', {
        topic,
        timestamp,
        offset,
        key: key.toString(),
        value: value.toString()
      });
    },
  })

  await Promise.all( conf.map( ({ topic }) => {
    return consumer.seek({ topic, partition: 0 , offset: -2})
  }))

}


test()

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
  //.forEach(event => console.log(event))

const device = main
  .filter(select('public.device'))
  .map(valueDecoder)
  //.forEach(event => console.log(event))

const merged = most.merge(gateway, device)
.forEach(event => console.log('merged',event))
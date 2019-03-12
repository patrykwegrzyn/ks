const { Kafka } = require('kafkajs')
const Events = require('events');
const most = require('most')

const kafka = new Kafka({
  clientId: 'my-app',
  brokers: ['localhost:29092']
})

// const admin = kafka.admin()
// const consumer = kafka.consumer({ groupId: 'test-group' })


class Consumer extends Events {
  constructor(kafka, conf) {
    super()
    this.config = conf
    this.consumer = kafka.consumer({ groupId: 'test-group' })
  }

  async initialize() {
    console.log()
    await this.consumer.connect()
    await this.consumer.subscribe({ topic: 'sensorData' })
    await this.consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        this.emit('message', { topic, partition, message })
      },
    })
    return this;
    console.log('here')
  }
}


//console.log(kafka)
async function test() {
  const c = await new Consumer(kafka).initialize()
  
  most.fromEvent('message', c)
  .forEach(event => { 
    console.log(event)
   })
//   await admin.connect()

//   const meta = await admin.fetchTopicsOffset({ topics: [
//           {
//         topic: 'sensorData',
//         partitions: [{ partition: 0 }],
//       }
//     ] })
//  console.log(JSON.stringify(meta, null, 2))
}


test()

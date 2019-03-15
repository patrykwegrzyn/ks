
const StreamFactory = require('./src/factory')

const options = {
  kafka: {
    clientId: 'my-app-3',
    brokers: ['xx.xxx.xxx.239:29092'],
    requestTimeout: 5000
  } 
};

const conf = [
  //{
//   kind:'stream',
//   topic: 'sensorData',
//   groupId:'sensorData-stream'
// },
{
  kind:'stream',
  topic: 'public.device',
},
{
  kind:'stream',
  topic: 'public.gateway',
}
]

const sf = new StreamFactory(options)

async function test() {
  const [ area, gateway] = await sf.initialize(conf)
  // area.forEach(event => {
  //   console.log('area event', event)
  // })
  // gateway.forEach(event => {
  //   console.log('gateway event', event)
  // })

console.log('consuming')

  

  // setTimeout(async () => {
  //   console.log('disconnecting')
  //   await sf.disconnect()
  // }, 5000)


}


test()



const errorTypes = ['unhandledRejection', 'uncaughtException']
const signalTraps = ['SIGTERM', 'SIGINT', 'SIGUSR2']

errorTypes.map(type => {
  console.log('here')
  process.on(type, async e => {
    try {
      console.log(`process.on ${type}`)
      console.error(e)
      await sf.disconnect()
      process.exit(0)
    } catch (_) {
      process.exit(1)
    }
  })
})

signalTraps.map(type => {
  process.once(type, async () => {
    try {
      console.log(`process.on traps ${type}`)
      await sf.disconnect();
      console.log('done')
    } finally {
      console.log('kill')
      process.exit(1)
      process.kill(process.pid, type)
    }
  })
})


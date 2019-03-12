
const StreamFactory = require('./src/factory')

const options = {
  kafka: {
    clientId: 'my-app-2',
    brokers: ['xx.xx.xx.239:29092']
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
  topic: 'public.area',
  groupId:'public.area-stream3'
}]

const sf = new StreamFactory(options)

async function test() {
  const [ area] = await sf.initialize(conf)
  // sensorData.forEach(event => {
  //   console.log('sensor event', event)
  // })

  area.forEach(event => {
    console.log('area event', event)
  })
}


test()


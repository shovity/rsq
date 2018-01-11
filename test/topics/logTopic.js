const { Topic, Consumer } = require('../..')
const fs = require('fs')

const logTopic = new Topic('logTopic', { numberParttions: 5 })

const writeFileConsumer = new Consumer()
const showLogComsumer = new Consumer()
const showLogComsumer2 = new Consumer()

writeFileConsumer.subscribe(logTopic)
showLogComsumer.subscribe(logTopic)

writeFileConsumer.onMessage((message, finish) => {
  const { timestamp, payload } = message

  fs.appendFile(
    __dirname + '/../log',
    `${timestamp}: ${JSON.stringify(payload)}\n`,
    (err) => {
      finish()
    }
  )
})

showLogComsumer.onMessage('show', (message, finish) => {
  const { timestamp, payload } = message
  console.log(`${timestamp}: ${JSON.stringify(message)}`)
  finish()
})


// otherConsumer with message type 'show'
// otherConsumer.onMessage('show', (message, finish) => {
//   const { timestamp, payload } = message
//   console.log(`2-${timestamp}: ${JSON.stringify(message)}`)
//   finish()
// })

// logTopic.removeConsumer('show')

module.exports = logTopic

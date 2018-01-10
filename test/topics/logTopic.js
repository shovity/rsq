const { Topic, Consumer } = require('../..')
const fs = require('fs')

const logTopic = new Topic('logTopic')

const writeFileConsumer = new Consumer()
const showLogComsumer = new Consumer()

writeFileConsumer.subscribe(logTopic, { parttion: [0] })
showLogComsumer.subscribe(logTopic, { parttion: [0] })

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

module.exports = logTopic

const Queue = require('.')

const queue = new Queue()

queue.newTopic('log').newStream('mysql')

queue.registHandle(
  [
    { topic: 'log', stream: 'mysql', type: 'create' },
  ],
  (message, done) => {
    const { payload, timestamp } = message
    const { something } = payload

    console.log('LOG CREATE: ' + JSON.stringify(message))
    done()
  }
)

queue.registHandle(
  [
    { topic: 'log', stream: 'mysql', type: 'remove' },
  ],
  (message, done) => {
    console.log('LOG REMOVE: ' + JSON.stringify(message))
    done()
  }
)

setInterval(() => {
  queue.push({
    topic: 'log',
    stream: 'mysql',
    type: (Math.random() < 0.5)? 'create' : 'remove',
    payload: { something: 'data' }
  })
}, 100)

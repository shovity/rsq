const Queue = require('.')

const queue = new Queue()

queue.newTopic('log')
  .newStream('redis')
  .newStream('mysql')

// const topic = queue.getStream('name')

queue.registHandle(
  [
    { topic: 'log', stream: 'mysql', type: 'create' },
    // { topic: 'log', stream: ['redis', 'mysql'], type: ['create', 'remove'] },
  ],
  (message, done) => {
    console.log('handle ' + JSON.stringify(message))
    done()
  }
)

queue.registHandle(
  [
    { topic: 'log', stream: 'mysql', type: 'remove' },
  ],
  (message, done) => {
    console.log('handle ' + JSON.stringify(message))
    done()
  }
)

for (let i = 0; i < 10; i++) {
  queue.push({
    topic: 'log',
    stream: 'mysql',
    // stream: ['mysql', 'redis'],  apply for all stream if lost, null, false
    type: (Math.random() < 0.5)? 'create' : 'remove',
    payload: 'payload ' + i
  })
}

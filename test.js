const Queue = require('.')

// create queue
const queue = new Queue()

// create topic
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

for (let i = 0; i < 2; i++) {
  queue.push({
    topic: 'log',
    stream: 'mysql',
    // stream: ['mysql', 'redis'],
    type: 'create',
    payload: 'payload ' + i
  })
}

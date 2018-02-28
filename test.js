const Queue = require('.')
const queue = new Queue()

// streams working concurently
const topic = queue.newTopic('log').newStream('mysql').newStream('otherStream')
queue.newTopic('log2').newStream('mysql').newStream('otherStream')

queue.registHandle(
  [
    { topic: 'log', stream: 'mysql', type: 'create' },
    // { topic: 'log', stream: ['mysql', 'outherStream'], type: ['create', 'otherType'] },
  ],
  (message, done) => {
    console.log(`${message.type} - ${message.id} - ${message.timestamp}`)
    done()
  }
)

queue.registHandle(
  [
    { topic: 'log', stream: 'mysql', type: 'remove' },
  ],
  (message, done) => {
    console.log(`${message.type} - ${message.id} - ${message.timestamp}`)
    done()
  }
)
queue.registHandle(
  [
    { topic: 'log2', stream: 'mysql', type: 'remove' },
  ],
  (message, done) => {
    console.log(`${message.type} - ${message.id} - ${message.timestamp}`)
    done()
  }
)

queue.on('error', (err, message) => {
  throw err
})

return console.log(queue.topics)

setInterval(() => {
  queue.push({
    topic: 'log',
    stream: 'mysql',
    type: (Math.random() < 0.5)? 'create' : 'remove',
    payload: { something: 'data' }
  }, (err) => {
    if (err) throw err
  })
}, 100)

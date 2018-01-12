const Queue = require('./Queue')
const Stream = require('./Stream')
const Topic = require('./Topic')

// create queue
const queue = new Queue()

// create topic
const topic = new Topic(
  'nameTopic',
  { expires: 12 * 60 * 60 }
)

// create stream
const fastStream = new Stream(
  'fastStream',
  'messageType',
  (message, done) => {
  setTimeout(() => {
    console.log('fastStream message: ' + JSON.stringify(message))
    done()
  }, 0)
})

// create stream
const slowStream = new Stream(
  'slowStream',
  'messageType',
  (message, done) => {
  setTimeout(() => {
    console.log('slowStream message: ' + JSON.stringify(message))
    done()
  }, 1000)
})

// add streams
topic.addStream(fastStream)
topic.addStream(slowStream)

// add topics
queue.addTopic(topic)

for (let i = 0; i < 3; i++) {
  queue.push({
    topic: 'nameTopic',
    type: 'messageType',
    payload: 'payload ' + i
  })
}

//                               |-> stream -> handle (mess type)
// mess -> |-> topic -> store -> |-> stream -> handle (mess type)
//         |                     |-> stream -> handle (mess type)
//         |
//         | -> topic

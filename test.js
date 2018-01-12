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
const stream = new Stream(
  'nameStream',
  'messageType',
  (message, done) => {
  console.log(JSON.stringify(message))
  done()
})

// create stream
const streamLong = new Stream(
  'longStream',
  'messageType',
  (message, done) => {
  setTimeout(() => {
    console.log('longer stream......')
    done()
  }, 1000)
})


topic.addStream(stream)
topic.addStream(streamLong)
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

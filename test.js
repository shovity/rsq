const Queue = require('.')

// create queue
const queue = new Queue()

// create topic
queue.newTopic('nameTopic')
  .newStream('nameStream', 'messageType')

// const topic = queue.getStream('name')

queue.registHandle(
  [
    { topic: 'nameTopic', streams: ['nameStream'] }
  ],
  (message, done) => {
    console.log(JSON.stringify(message))
    done()
  }
)

queue.push({
  topic: 'nameTopic',
  type: 'messageType',
  payload: 'payload '
})

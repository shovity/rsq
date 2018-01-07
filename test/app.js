const Queue = require('../')

const topic1 = require('./topics/topic1')
const topic2 = require('./topics/topic2')


const queue = new Queue()

queue.addTopic(topic1)
queue.addTopic(topic2)

queue.push({
  topic: 'topic1',
  partition: 0,
  payload: 'some data for topic 1'
})

queue.push({
  topic: 'topic1',
  partition: 0,
  payload: 'some data for topic 2'
})

queue.push({
  topic: 'topic1',
  partition: 0,
  payload: 'some data for topic 1'
})

queue.push({
  topic: 'topic1',
  partition: 0,
  payload: 'some data for topic 2'
})

queue.push({
  topic: 'topic1',
  partition: 0,
  payload: 'some data for topic 1'
})

queue.push({
  topic: 'topic1',
  partition: 0,
  payload: 'some data for topic 2'
})

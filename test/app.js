const Queue = require('../')

const logTopic = require('./topics/logTopic')
const topic2 = require('./topics/topic2')


const queue = new Queue()

queue.addTopic(logTopic)
queue.addTopic(topic2)


for (let i = 0; i < 10000; i++) {
  queue.push({
    topic: 'logTopic',
    type: (i % 100 === 0)? 'show' : '',
    payload: 'Lorem ipsumdolor sit amet, consectetur adipisicing elit ' + i
  })
}

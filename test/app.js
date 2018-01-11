const Queue = require('../')

const logTopic = require('./topics/logTopic')
const longerTopic = require('./topics/longerTopic')

const queue = new Queue()

queue.addTopic(logTopic)
queue.addTopic(longerTopic)

// queue.notify()
// queue.removeTopic('longerTopic')
// queue.shutdow()

for (let i = 0; i < 10000; i++) {
  queue.push({
    topic: 'logTopic',
    type: (i % 100 === 0)? 'show' : '',
    payload: 'Lorem ipsumdolor sit amet, consectetur adipisicing elit ' + i
    // parttion: 0,   // default: random
    // timestamp: 0,  // default: current timestamp
  })

  if (i < 10) {
    queue.push({
      topic: 'longerTopic',
      type: '',
      payload: 'message for longerTopic' + i
    })
  }
}

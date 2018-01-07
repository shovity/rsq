const { Topic, Partition, Consumer } = require('../..')

const topic1 = new Topic('topic1')
const consumer1 = new Consumer()
const consumer3 = new Consumer()

consumer1.subscribe(topic1, { parttion: [0] })
consumer3.subscribe(topic1, { parttion: [0] })

consumer1.onMessage((message, feedback) => {
  setTimeout(() => {
    console.log('c1 handle for ' + JSON.stringify(message))
    feedback()
  }, 1000)
})

consumer3.onMessage((message, feedback) => {
  setTimeout(() => {
    console.log('c3 handle for ' + JSON.stringify(message))
    feedback()
  }, 2000)
})

module.exports = topic1

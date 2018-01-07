const { Topic, Partition, Consumer } = require('../..')

const topic2 = new Topic('topic2')
const consumer2 = new Consumer()

consumer2.subscribe(topic2, { parttion: [0] })

consumer2.onMessage((message, feedback) => {
  console.log('c2 handle for ' + JSON.stringify(message))
  feedback()
})

module.exports = topic2

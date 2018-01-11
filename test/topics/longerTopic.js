const { Topic, Partition, Consumer } = require('../..')

const longerTopic = new Topic('longerTopic')
const workHard = new Consumer()

workHard.subscribe(longerTopic)

workHard.onMessage((message, feedback) => {
  setTimeout(() => {
    console.log('longerTopic, message: ' + JSON.stringify(message))
    feedback()
  }, 1000)
})

module.exports = longerTopic

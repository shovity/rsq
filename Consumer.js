/**
 * Consumer subscribe to topic for get message
 */

class Consumer {
  constructor() {
    this.handle = (message, finish) => {
      console.log('consumer not regist onMessage')
      finish()
    }
    this.messageType = ''
  }

  subscribe(topic, options) {
    const { partitions = [0] } = options || {}

    if (topic.partitions.length !== 0) {
      // handles inited
      partitions.forEach(p => {
        if (topic.partitions[p]) {
          topic.partitions[p].subscribers.push(this)
        }
      })
    } else {
      // handles uninited
      partitions.forEach(p => {
        topic.subscribersKeeper.push({
          subscriber: this,
          partition: p
        })
      })
    }
  }

  onMessage(messageType, callback) {
    const cb = (typeof messageType === 'function')? messageType : callback
    this.messageType = (typeof messageType === 'string')? messageType : ''
    this.handle = cb
  }
}

module.exports = Consumer

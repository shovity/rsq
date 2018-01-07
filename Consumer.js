class Consumer {
  constructor() {
    this.handle = () => {
      console.log('consumer not regist onMessage')
    }
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

  onMessage(options, callback) {
    const cb = (typeof options === 'function')? options : callback
    this.handle = cb
  }
}

module.exports = Consumer

const Partition = require('./Partition')

class Topic {
  constructor(name, options) {
    // destructuring options
    const { numberParttions } = options || {}

    this.name = name
    // default number of partitions is 1
    this.numberParttions = numberParttions || 1
    this.partitions = []
    this.redisClient = null
    this.subscribersKeeper = []
  }

  setRedisClient(redisClient) {
    this.redisClient = redisClient
    // initial partitions
    for (let i = 0; i < this.numberParttions; i++) {
      const redisKey = `_topic_${this.name}_${i}`
      this.partitions.push(new Partition({ redisClient, redisKey }))
    }
    // add handle in handleKeeper
    this.subscribersKeeper.forEach(h => {
      if (this.partitions[h.partition]) {
        this.partitions[h.partition].subscribers.push(h.subscriber)
      } else {
        console.log('try to add handle to partiton not exist')
      }
    })
  }

  push(message) {
    const parttionIndex = message.parttion || 0
    const partition = this.partitions[parttionIndex] || partitions[0]
    partition.push(message)
  }
}

module.exports = Topic

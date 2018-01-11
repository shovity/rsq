const Partition = require('./Partition')
const Consumer = require('./Consumer')

class Topic {
  /**
   * @param {string} name
   * @param {Object} options
   *  numberParttions: {number} number of partiton, defautl 1
   *  concurrency: {Mixed} boolean or array, consumer handle concurrency, default ture
   */
  constructor(name, options) {
    // destructuring options
    const {
      numberParttions,
      concurrency
    } = options || {}

    this.redisClient = null
    // default number of partitions is 1
    this.numberParttions = numberParttions || 1
    this.name = name
    this.partitions = []
    this.subscribers = []
    this.concurrency = concurrency || true
  }

  /**
   * Set redis client for all partiton
   */
  setRedisClient(redisClient) {
    this.redisClient = redisClient
    // initial partitions
    for (let i = 0; i < this.numberParttions; i++) {
      const redisKey = `_topic_${this.name}_${i}`

      const options = {
        redisClient,
        redisKey,
        topic: this,
        concurrency: (typeof this.concurrency === 'boolean')? this.concurrency : !!this.concurrency[i]
      }

      this.partitions.push(new Partition(options))
    }
  }

  /**
   * Push message to partiton
   * random parttion if message.parttion is undefined
   * @param  {Object} message
   */
  push(message) {
    const parttionIndex = message.parttion || Math.floor(Math.random()*this.partitions.length)
    const partition = this.partitions[parttionIndex] || partitions[0]
    partition.push(message)
  }

  /**
   * Notify changed to all partitions
   */
  notify() {
    this.partitions.forEach(partiton => {
      partiton.notify()
    })
  }

  /**
   * Remove consumer
   * @param  {Mixed} instanceOrType stirng (messageType) or Object (Consumer)
   */
  removeConsumer(instanceOrType) {
    if (typeof instanceOrType === 'string') {
      this.subscribers = this.subscribers.filter(sub => {
        return sub.messageType !== instanceOrType
      })
    } else if (instanceOrType instanceof Consumer) {
      let consumerIndex = this.subscribers.indexOf(instanceOrType)
      if (consumerIndex !== -1) this.subscribers.splice(consumerIndex, 1)
    } else {
      throw Error('Remove consumer err. Param must instance of Consumer or type (string)')
    }
  }
}

module.exports = Topic

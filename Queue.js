const redis = require('redis')
const Topic = require('./Topic')
const Partition = require('./Partition')
const Consumer = require('./Consumer')

class Queue {
  constructor(config) {
    const {
      redisOptions,
      redisClient,
    } = config || {}

    this.redisClient = redisClient || this.createRedisClient(redisOptions)
    this.topics = []
  }

  addTopic(topic) {
    if (topic instanceof Topic) {
      this.topics.push(topic)
      topic.setRedisClient(this.redisClient)
    } else {
      throw new Error('topic must be instance of Topic')
    }
  }

  /**
   * Pick topic
   * @param  {Object} message
   * @return {Topic}
   */
  detectTopic(message) {
    return this.topics.find(topic => topic.name === message.topic)
  }

  push(message) {
    const topic = this.detectTopic(message)
    topic.push(message)
  }

  /**
   * Create redis connect
   */
  createRedisClient(options) {
    const client = redis.createClient(options)

    client.on('connect', () => {
      // console.log("redis server connected")
    })

    client.on('error', (err) => {
      console.log(err)
    })

    return client
  }
}

Queue.Topic = Topic
Queue.Partition = Partition
Queue.Consumer = Consumer

module.exports = Queue

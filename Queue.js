const redis = require('redis')
const Topic = require('./Topic')
const Consumer = require('./Consumer')

class Queue {
  /**
   * @param {Object} config
   *  if lost redisClient create redisClient with redisOptions
   */
  constructor(config) {
    const {
      redisOptions,
      redisClient,
    } = config || {}

    this.redisClient = redisClient || this.createRedisClient(redisOptions)
    this.topics = []
  }

  /**
   * Add topic to queue and boot that
   * @param {Object} topic
   */
  addTopic(topic) {
    if (topic instanceof Topic) {
      this.topics.push(topic)
      topic.setRedisClient(this.redisClient)
      topic.notify()
    } else {
      throw new Error('Topic must be instance of Topic')
    }
  }

  /**
   * Pick topic
   * @param  {Object} message
   * @return {Topic} null if not found
   */
  detectTopic(message) {
    const topic = this.topics.find(topic => {
      return topic.name === message.topic
    })
    return topic
  }

  /**
   * Push message
   * ignore message if message topic not match
   * @param  {Object} message
   */
  push(message) {
    const topic = this.detectTopic(message)
    if (topic) topic.push(message)
  }

  /**
   * Create redis connect
   * @param {Object} options redis client options
   */
  createRedisClient(options) {
    const client = redis.createClient(options)

    client.on('connect', () => {
      // console.log("redis server connected")
    })

    client.on('error', (err) => {
      throw err
    })

    return client
  }

  /**
   * Notify changed to all topics
   * @return {[type]} [description]
   */
  notify() {
    this.topics.forEach(topic => {
      topic.notify()
    })
  }

  /**
   * Remve topic
   * @param  {Mixed} topicName String name or array of String name
   */
  removeTopic(topicName) {
    let filterList = []
    if (!Array.isArray(topicName)) filterList = [topicName]
    this.topics = this.topics.filter(topic => {
      return filterList.indexOf(topic.name) === -1
    })
  }

  /**
   * Remove redis connect
   * @return {[type]} [description]
   */
  shutdow() {
    this.redisClient.quit()
  }
}

Queue.Topic = Topic
Queue.Consumer = Consumer

module.exports = Queue

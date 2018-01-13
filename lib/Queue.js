const redis = require('redis')

const Topic = require('./Topic')
const Stream = require('./Stream')

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
   * Regist handle for streams
   */
  registHandle(targets, handle) {
    targets.forEach(target => {
      const topicName = target.topic
      const streamNames = target.streams

      const topic = this.topics.find(t => t.name = topicName)

      if (!topic) throw new Error('registHandle fail, topic not exists')


      topic.streams.forEach(stream => {
        if (streamNames.indexOf(stream.name) !== -1) stream.handle = handle
      })
    })
  }

  /**
   * Create and add topic
   * @param  {string} name
   * @param  {Object} options
   * @return {Topic}
   */
  newTopic(name, options) {
    const topic = new Topic( name, { redisClient: this.redisClient, ...options || {} })
    this.topics.push(topic)
    return topic
  }

  /**
   * @param  {string} name
   * @return {Topic}
   */
  getTopic(name) {
    return this.topics.find(t => t.name === name)
  }

  /**
   * Pick topic
   * @param  {Object} message
   * @return {Topic} null if not found
   */
  exchange(message) {
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
    const topic = this.exchange(message)
    if (topic) topic.push(message)
  }

  /**
   * Create redis connect
   * @param {Object} options redis client options
   * @return {RedisClient} redis client
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
   */
  shutdow() {
    this.redisClient.quit()
  }
}

Queue.Topic = Topic
Queue.Stream = Stream

module.exports = Queue

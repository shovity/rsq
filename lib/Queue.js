const redis = require('redis')
const Topic = require('./Topic')
const Stream = require('./Stream')

/**
 * Keys in redis
 * strem : {queue-name}:{topic-name}:stream:{stream-name}
 * store : {queue-name}:{topic-name}:store:{serial}
 * serial: {queue-name}:{topic-name}:serial
 */

class Queue {
  /**
   * @param {Object} config
   *  if redisClient is undefined, create redisClient with redisOptions
   */
  constructor(name, config = {}) {
    this.redisClient = config.redisClient || this.createRedisClient(config.redisOptions)
    this.name = name || 'queue'
    this.config = config
    this.topics = []
  }

  /**
   * Regist handle for streams
   * @param {Array<Object>} targets [{ topic: string, streams: string or Array<string>, types: string or Array<string> }]
   * @param {Function} handle
   */
  registHandle(targets, handle) {
    // each targets
    targets.forEach(target => {
      const topicName = target.topic
      const streamNames = (typeof target.stream === 'string')? [target.stream] : target.stream
      const typeNames = (typeof target.type === 'string')? [target.type] : target.type

      // find topic by name
      const topic = this.topics.find(t => t.name = topicName)
      if (!topic) throw new Error('registHandle failed, topic not exists')

      // regist handle for each streams
      topic.streams.forEach(stream => {
        if (streamNames.indexOf(stream.name) === -1) return

        typeNames.forEach(typeName => {
          stream.handles.push({ type: typeName, handle })
        })
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
    const topic = new Topic(
      name,
      {
        redisClient: this.redisClient,
        queueName: this.name,
        // override global config
        ...this.config,
        // override own config
        ...options || {}
      }
    )

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
   * Pick topic for push message
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
    if (topic) {
      topic.push(message)
    } else {
      console.log(`"${message.topic}" not match any topic, message was lost`)
    }
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
   * Auto clean prefix queueName:*
   */
  clean(callback) {
    this.redisClient.keys(`${this.name}:*`,  (err, reply) => {
      if (err) return callback(err)
      this.redisClient.del(reply, callback)
    })
  }

  /**
   * Remove redis connect
   */
  shutdow() {
    this.redisClient.quit()
  }
}

module.exports = Queue

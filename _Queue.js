const redis = require('redis')
const Topic = require('./Topic')
const Partition = require('./Partition')

const MESSAGE_QUEUE = 'MESSAGE_QUEUE'

class Queue {
  constructor(config) {
    const {
      redisOptions,
      redisClient,
      queueKey
    } = config || {}

    this.queueKey = queueKey || MESSAGE_QUEUE
    this.redisClient = redisClient || this.createRedisClient(redisOptions)

    this.topics = []
    this.events = [] // { name: '', handles: [] }
    this.currentTotal = 0
    this.isBusy = false
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

  tok() {
    // up busy flag
    this.isBusy = true
    // get message
    this.pop((err, message) => {
      if (err) throw err

      if (!message) {
        // queue is null
        this.isBusy = false
        console.log('queue is null')
        return
      }

      // pick topic
      const topic = this.detectTopic(message)

      // if topic not match
      if (!topic) {
        setTimeout(() => {
          this.tok()
        })
        return console.log('topic not match')
      }

      // handle
      topic.handle(message, (feedback) => {
        // finished callback
        if (feedback) console.log('feedback=' + feedback)
        setTimeout(() => {
          this.tok()
        })
      })
    })
  }

  /**
   * Regist event listener
   * @param {String} name
   * @param {Rest} handles rest of function
   */
  addEvent(name, ...handles) {
    handles.forEach(handle => {
      if (typeof handle !== 'function') throw new Error('Handle must be function')
    })

    const existsEventIndex = this.events.findIndex(event => event.name === name)
    if (existsEventIndex === -1) {
      this.events.push({ name, handles })
    } else {
      this.events[existsEventIndex].handles.push(...handles)
    }
  }

  /**
   * Remove event listener
   * @param {String} name
   * @param {Rest} handles rest of function name
   */
  removeEvent(name, ...handleNames) {
    handleNames.forEach(handle => {
      if (typeof handle !== 'string') throw new Error('HandleName must be string')
    })

    const eventIndex = this.events.findIndex(event => event.name === name)

    if (eventIndex === -1) {
      // event not exists
    } else if (handleNames.length === 0) {
      // remove all handles
      this.events.splice(eventIndex, 1)
    } else {
      this.events[eventIndex].handles = this.events[eventIndex].handles.filter(handle => {
        return handleNames.indexOf(handle.name) === -1
      })
    }
  }

  /**
   * Create message json string
   * @param  {String} name
   * @return {String}
   */
  messageCreator(message) {
    return JSON.stringify(message)
  }

  /**
   * Push a message to queue
   * @param  {String}   name
   * @param  {Object}   data
   * @param  {Object}   [options] { notify (default true) }
   * @param  {Function} [callback]
   */
  push(name, data, options, callback) {
    const cb = (typeof options === 'function')? options : callback
    const { notify = true } = options || {}

    this.redisClient.rpush(this.queueKey, this.messageCreator(name, data), (err, reply) => {
      if (typeof cb === 'function') cb(err, reply)
    })
  }

  /**
   * @return {Object}
   */
  pop(callback) {
    this.currentTotal--
    this.redisClient.lpop(this.queueKey, (err, reply) => {
      if (err) {
        callback(err, null)
      } else {
        const message = JSON.parse(reply)
        callback(null, message)
      }
    })
  }

  /**
   * Tok
   */
  tik() {
    this.isBusy = true
    this.pop((err, message) => {
      if (err) throw err

      if (!message) {
        // queue is null
        this.isBusy = false
        // console.log('queue is null')
        return
      }

      // handle event
      const { name, data } = message
      const event = this.events.find(event => event.name === name)

      if (!event) {
        setTimeout(() => {
          this.tik()
        })
        return console.log('event not exsits')
      }

      const handles = event.handles
      const length = handles.length

      let current = 0
      const next = () => {
        if (current > length-2) {
          const handle = handles[current++]
          if (typeof handle === 'function') {
            handle(data, this.tik.bind(this), reject)
          } else {
            console.log({ length, current, ok: current === length-1  })
            console.log('handle err: ' + handle)
          }
        } else {
          const handle = handles[current++]
          if (typeof handle === 'function') {
            handle(data, next, reject)
          } else {
            console.log({ length, current, ok: current === length-1  })
            console.log('handle err: ' + handle)
          }
        }
      }

      const reject = () => {
        console.log('reject message: ', message)
        this.push(name, data)
      }

      next()
    })
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
      console.log("connect redis server error: " + err)
    })

    return client
  }

  /**
   * Remove queue
   */
  clean(callback) {
    this.redisClient.del(this.queueKey, callback)
  }

  /**
   * Close connect redis server
   */
  shutdown() {
    this.redisClient.quit()
  }
}

Queue.Topic = Topic
Queue.Partition = Partition

module.exports = Queue

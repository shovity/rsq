
class Partition {
  /**
   * @param {Object} options required { redisKey, redisClient, topic }
   *  concurrency: handle consumer concurrency, default true
   */
  constructor(options) {
    const {
      redisKey,
      redisClient,
      topic,
      concurrency
    } = options

    this.redisClient = redisClient
    this.redisKey = redisKey
    this.isBusy = false
    this.topic = topic

    this.concurrency = concurrency || true
  }

  /**
   * Create message and add default options
   * @return {String}
   */
  messageCreator(message) {

    return JSON.stringify({
      ...message,
      // default options
      timestamp: message.timestamp || + new Date()
    })
  }

  /**
   * Push message to redis list and notify
   * @param  {Object} message
   * @param  {Object} options
   */
  push(message, options) {
    if (!this.redisClient) throw new Message('redis client null')
    const { notify = true } = options || {}
    // push to redis
    this.redisClient.rpush(this.redisKey, this.messageCreator(message), (err, reply) => {
      if (err) throw err
      notify && this.notify()
    })
  }

  /**
   * @return {Object}
   */
  pop(callback) {
    this.redisClient.lpop(this.redisKey, (err, reply) => {
      if (err) {
        callback(err, null)
      } else {
        const message = JSON.parse(reply)
        callback(null, message)
      }
    })
  }

  run() {
    // up busy flag
    this.isBusy = true
    // get message
    this.pop((err, message) => {
      if (err) throw err

      if (!message) {
        // partition is null
        this.isBusy = false
        return
      }

      const subscribers = this.topic.subscribers
      const length = subscribers.length

      if (this.concurrency) {
        // Handle concurrency
        // remove message if every is done
        let unfinish = 0
        const finish = (feedback) => {
          // handle feedback
          unfinish--
          if (unfinish <= 0) {
            setTimeout(() => {
              this.run()
            })
          }
        }

        const consumerFilted = []
        subscribers.forEach(subscribe => {
          const { messageType } = subscribe
          if (!messageType || messageType === message.type) {
            unfinish++
            consumerFilted.push(subscribe)
          }
        })

        if (consumerFilted.length === 0) {
          // Message not match any consumer
          // remove !!!
          console.log('message not match any consumer, remove message')
          finish()
        }

        consumerFilted.forEach(consumer => {

          consumer.handle(message, finish)
        })
      } else {
        // Handle Step by step
        let current = 0
        const next = () => {
          if (current > length-2) {
            // end of subscribers
            const subscriber = subscribers[current++]

            // pass handle if messageType exists and not match
            if (subscriber.messageType && subscriber.messageType !== message.type) {
              setTimeout(() => {
                this.run()
              })
              return
            }

            const handle = subscriber.handle

            if (typeof handle === 'function') {
              handle(message, () => {
                this.run()
              })
            } else {
              console.log('handle must be a function')
            }
          } else {
            // not end of subscribers
            const subscriber = subscribers[current++]

            // pass handle if messageType exists and not match
            if (subscriber.messageType && subscriber.messageType !== message.type) {
              setTimeout(() => {
                next()
              })
              return
            }

            const handle = subscriber.handle
            if (typeof handle === 'function') {
              handle(message, next)
            } else {
              console.log('handle must be a function')
            }
          }
        }

        next()
      }
      //-- end pop
    })
  }

  notify() {
    if (!this.isBusy) this.run()
  }
}

module.exports = Partition

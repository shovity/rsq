
class Partition {
  constructor(options) {
    const {
      redisKey,
      redisClient,
    } = options

    this.redisClient = redisClient
    this.redisKey = redisKey
    this.subscribers = []
    this.isBusy = false
  }

  /**
   * @return {String}
   */
  messageCreator(message) {

    return JSON.stringify({
      ...message,
      timestamp: message.timestamp || + new Date()
    })
  }

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
    this.currentTotal--
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

      const subscribers = this.subscribers
      const length = subscribers.length

      // Handle concurrency
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
        console.log('message not match any consumer')
        finish()
      }

      consumerFilted.forEach(consumer => {
        consumer.handle(message, finish)
      })


      // Handle Step by step
      // let current = 0
      // const finish = () => {
      //   if (current > length-2) {
      //     const handle = subscribers[current++].handle
      //
      //     if (typeof handle === 'function') {
      //       handle(message, () => {
      //         this.run()
      //       })
      //     } else {
      //       console.log('handle must be a function')
      //     }
      //   } else {
      //     const handle = subscribers[current++].handle
      //     if (typeof handle === 'function') {
      //       handle(message, finish)
      //     } else {
      //       console.log('handle must be a function')
      //     }
      //   }
      // }
      //
      // finish()
    })
  }

  notify() {
    if (!this.isBusy) this.run()
  }
}

module.exports = Partition

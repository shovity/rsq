
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
    return JSON.stringify(message)
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
      const { payload } = message

      let current = 0
      const next = () => {
        if (current > length-2) {
          const handle = subscribers[current++].handle
          if (typeof handle === 'function') {
            handle(payload, () => {
              this.run()
            })
          } else {
            console.log('handle err: ' + handle)
          }
        } else {
          const handle = subscribers[current++].handle
          if (typeof handle === 'function') {
            handle(payload, next)
          } else {
            console.log('handle err: ' + handle)
          }
        }
      }

      const reject = () => {
        console.log('reject message: ', message)
      }

      next()

      // handle
      // this.subscribers.forEach(sub => {
      //   sub.handle(message, (msgfb) => {
      //     // finished callback
      //     if (msgfb) console.log('msgfb=' + feedback)
      //     setTimeout(() => {
      //       this.run()
      //     })
      //   })
      // })
    })
  }

  notify() {
    if (!this.isBusy) this.run()
  }
}

module.exports = Partition

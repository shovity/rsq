class Stream {
  /**
   * @param {String} name
   * @param {String} messageType apply all message type if null
   * @param {Object} options required { keyStream, redisClient, genKeySeri}
   */
  constructor(name, messageType, options) {
    this.name = name
    this.messageType = messageType

    const {
      keyStream,
      redisClient,
      genKeySeri
    } = options || {}

    // required options
    this.keyStream = keyStream
    this.redisClient = redisClient
    this.genKeySeri = genKeySeri

    this.handle = (message, done) => {
      console.log('the stream does not set the handle, the message will be lost')
      done()
    }

    // flags
    this.isWorking = false

    //binding
    this.done = this.done.bind(this)
  }

  /**
   * @param  {number}   seriNumber
   * @param  {Function} callback   callback(err, message)
   */
  getMessageFromStore(seriNumber, callback) {
    this.redisClient.get(this.genKeySeri(seriNumber), (err, reply) => {
      if (err) throw err

      try {
        const message = JSON.parse(reply)
        callback(null, message)
      } catch (err) {
        callback(err, null)
      }
    })
  }

  /**
   * Push next work to task queue
   * @param  {Object}   feedback
   */
  done(feedback) {
    // handle feedback

    // next work
    setTimeout(() => {
      this.work()
    })
  }

  work() {
    if (!this.keyStream) throw Error('Missing key stream')
    if (!this.redisClient) throw Error('Missing redis client')

    this.isWorking = true

    this.redisClient.lpop(this.keyStream, (err, reply) => {
      const seriNumber = +reply

      // check end of stream
      if (!seriNumber) {
        this.isWorking = false
        return
      }

      // get message
      this.getMessageFromStore(seriNumber, (err, message) => {
        if (err) throw err
        if (!message) {
          // message expired or invalid
          this.done()
        } else {
          this.handle(message, this.done)
        }
      })

    })
  }

  notify() {
    if (this.isWorking) return
    this.work()
  }
}

module.exports = Stream

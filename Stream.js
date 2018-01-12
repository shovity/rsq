class Stream {
  constructor(name, messageType, handle) {
    this.name = name
    this.messageType = messageType
    this.handle = handle

    // initial when Topic.addStream
    this.keyStream = null
    this.redisClient = null
    this.genKeySeri = null

    // flags
    this.isWorking = false

    //binding
    this.done = this.done.bind(this)
  }

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

  done(feedback) {
    // handle feedback
    //
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

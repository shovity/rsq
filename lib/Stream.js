class Stream {
  /**
   * @param {String} name
   * @param {String} messageType apply all message type if null
   * @param {Object} options required { keyStream, redisClient, genKeySerial}
   */
  constructor(name, options) {
    this.name = name

    const {
      keyStream,
      redisClient,
      genKeySerial
    } = options || {}

    // required options
    if (!keyStream) throw Error('Missing key stream')
    if (!redisClient) throw Error('Missing redis client')
    if (!genKeySerial) throw Error('Missing genKeySerial')
    this.keyStream = keyStream
    this.redisClient = redisClient
    this.genKeySerial = genKeySerial

    // { type: string, handle: Function }
    this.handles = []

    // flags
    this.isWorking = false

    // binding
    this.work = this.work.bind(this)
  }

  /**
   * @param  {number}   serial
   * @param  {Function} callback   callback(err, message)
   */
  getMessageFromStore(serial, callback) {
    this.redisClient.get(this.genKeySerial(serial), (err, reply) => {
      if (err) {
        callback(err)
      } else {
        const message = JSON.parse(reply)
        callback(null, message)
      }
    })
  }


  /**
   * Worker recursive using task queue
   */
  work() {
    this.isWorking = true
    this.redisClient.lpop(this.keyStream, (err, reply) => {
      if (err) throw err
      const serial = +reply

      // check end of stream
      if (!serial) {
        this.isWorking = false
        return
      }

      // get message
      this.getMessageFromStore(serial, (err, message) => {
        if (err) throw err
        if (!message) {
          // message expired or invalid
          setTimeout(this.work)
        } else {
          const done = (err) => {
            if (err) {
              // handle err
              console.log(err.msg)
              setTimeout(this.work)
            } else {
              setTimeout(this.work) // low priority, next work
              // setImmediate(this.work) // medium priority, next work
              // process.nextTick(this.work) // height priority, next work
            }
          }

          // pick handle type
          const type = message.type
          const handle = this.handles.find(handle => handle.type === type)

          if (handle) {
            handle.handle(message, done.bind(this))
          } else {
            // not match any handle type
            done({ msg: 'message not match any handle type registed, message lost' })
          }
        }
      })
    })
  }

  /**
   * Notify worker
   */
  notify() {
    if (this.isWorking) return
    this.work()
  }
}

module.exports = Stream

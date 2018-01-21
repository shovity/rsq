class Stream {
  /**
   * @param {String} name
   * @param {String} messageType apply all message type if null
   * @param {Object} config required { keyStream, redisClient, genKeySerial}
   */
  constructor(name, config = {}) {
    this.name = name

    // destructuring config
    const { keyStream, redisClient, genKeySerial, priority } = config

    // required config
    if (!keyStream) throw Error('Missing key stream')
    if (!redisClient) throw Error('Missing redis client')
    if (!genKeySerial) throw Error('Missing genKeySerial')
    this.keyStream = keyStream
    this.redisClient = redisClient
    this.genKeySerial = genKeySerial

    // options
    this.priority = priority || 0

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
    this.redisClient.get(this.genKeySerial(serial), (err, rep) => {
      if (err) {
        callback(err)
      } else {
        const message = JSON.parse(rep)
        callback(null, message)
      }
    })
  }

  /**
   * Worker recursive using task queue
   */
  work() {
    this.isWorking = true
    this.redisClient.lpop(this.keyStream, (err, rep) => {
      if (err) throw err
      const serial = +rep

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
              console.log(err)
              setTimeout(this.work)
            } else {
              this.registNextWork(this.work)
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
   * Push next work to task queue
   * @param  {Fuction} work
   * @param  {Rest} args args of work
   */
  registNextWork(work, ...args) {
    switch (this.priority) {
      case 1:
        setImmediate(work, ...args)
        break;
      case 2:
        process.nextTick(work, ...args)
        break;

      default:
        setTimeout(work, 0, ...args);
    }
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

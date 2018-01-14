const Stream = require('./Stream')

class Topic {
  /**
   * @param {string} name
   * @param {Object} options
   *  expires: time expires (secends) default: 1 day
   */
  constructor(name, options) {
    // destructuring options
    const {
      expires,
      redisClient,
      queueName
    } = options || {}

    this.redisClient = redisClient
    this.name = name
    this.queueName = queueName || 'queue'
    this.expires = expires || 24 * 60 * 60
    this.streams = []

    // constants
    this.QUEUE_SERIAL = `${this.queueName}_${this.name}_serial`

    // binding
    this.genKeySerial = this.genKeySerial.bind(this)
  }

  /**
   * Generate seri key by name, serial
   * @param  {number} serial
   * @return {string}
   */
  genKeySerial(serial) {
    return `${this.queueName}_${this.name}_store_${serial}`
  }

  /**
   * Generate stream key by name
   * @param  {string} name
   * @return {string}
   */
  genKeyStream(name) {
    return `${this.queueName}_${this.name}_${name}`
  }

  /**
   * Create and add stream
   * @param {Stream} stream
   */
  newStream(name, options) {
    const stream = new Stream(
      name,
      {
        keyStream: this.genKeyStream(name),
        redisClient: this.redisClient,
        genKeySerial: this.genKeySerial,
        ...options || {}
      }
    )

    this.streams.push(stream)
    stream.notify()
    return this
  }

  /**
   * Create message and add default options
   * default options will override own options if exists
   * @return {string}
   */
  messageCreator(message, id) {

    return JSON.stringify({
      ...message,
      // default options
      id,
      timestamp: message.timestamp || + new Date()
    })
  }

  /**
   * Store message and push to streams
   * @param  {Object} message
   */
  push(message) {
    this.redisClient.incr(this.QUEUE_SERIAL, (err, reply) => {
      if (err) throw err
      let serial = 0
      if (reply) serial = +reply

      // gen multi commands
      let commandChain = []

      // reset serial if it's not safe
      if (serial >= Number.MAX_SAFE_INTEGER) commandChain.push(['del', this.QUEUE_SERIAL])

      // store message
      commandChain.push([
        'SET',
        this.genKeySerial(serial),
        this.messageCreator(message, serial),
        'EX',
        this.expires
      ])

      // push message to streams
      let notMatchAnyStream = true
      this.streams.forEach(stream => {
        let streams = message.stream
        if (!message.stream) {
          streams = false
        } else if (typeof message.stream === 'string') {
          streams = [message.stream]
        } else if (!Array.isArray(message.stream)) {
          return console.log('message.stream must be string or array of string')
        }

        if (!streams || streams.indexOf(stream.name) !== -1) {
          notMatchAnyStream = false
          commandChain.push([
            'RPUSH',
            this.genKeyStream(stream.name),
            serial
          ])
        }
        //- end forEach
      })

      if (notMatchAnyStream) {
        return console.log('message not match any stream, message lost')
      }

      // execute commandChain
      this.redisClient.multi(commandChain).exec((err, reply) => {
        if (err) throw err
        this.notify()
      })
      //- end incr
    })
  }

  /**
   * Notify changed to all partitions
   */
  notify() {
    this.streams.forEach(stream => {
      stream.notify()
    })
  }

  /**
   * Remove stream
   * @param  {Mixed} instanceOrName stirng (stream name) or Object (Stream)
   */
  removeStream(instanceOrName) {
    if (typeof instanceOrName === 'string') {
      this.streams = this.streams.filter(stream => {
        return stream.name !== instanceOrName
      })
    } else if (instanceOrName instanceof Stream) {
      let streamIndex = this.streams.indexOf(instanceOrName)
      if (streamIndex !== -1) {
        this.streams.splice(streamIndex, 1)
      } else {
        console.log('ERR: Remve stream err, stream not exists')
      }
    } else {
      throw Error('Remove stream err. Param must instance of Stream or stream name')
    }
  }
}

module.exports = Topic

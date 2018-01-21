const Stream = require('./Stream')

class Topic {
  /**
   * @param {string} name
   * @param {Object} config
   *  expires: time expires (secends) default: 2 day
   */
  constructor(name, config = {}) {
    this.name = name
    this.redisClient = config.redisClient
    this.queueName = config.queueName || 'queue'
    this.expires = config.expires || 48 * 60 * 60
    this.streams = []

    // Constants
    // {queue-name}:{topic-name}:serial
    this.QUEUE_SERIAL = `${this.queueName}:${this.name}:serial`

    // binding
    this.genKeySerial = this.genKeySerial.bind(this)
  }

  /**
   * Generate seri key by name, serial
   * @param  {number} serial
   * @return {string}
   */
  genKeySerial(serial) {
    // {queue-name}:{topic-name}:store:{serial}
    return `${this.queueName}:${this.name}:store:${serial}`
  }

  /**
   * Generate stream key by name
   * @param  {string} name
   * @return {string}
   */
  genKeyStream(name) {
    // {queue-name}:{topic-name}:stream:{stream-name}
    return `${this.queueName}:${this.name}:${name}`
  }

  /**
   * Create and add stream
   * @param  {string} name
   * @param  {Object} config
   */
  newStream(name, config) {
    const stream = new Stream(
      name,
      {
        keyStream: this.genKeyStream(name),
        redisClient: this.redisClient,
        genKeySerial: this.genKeySerial,
        ...config || {}
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

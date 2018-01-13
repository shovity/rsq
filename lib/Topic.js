const Stream = require('./Stream')

const QUEUE_SERI_NUMBER = 'queue_seri_number'

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
      redisClient
    } = options || {}

    this.redisClient = redisClient || null
    this.name = name
    this.expires = expires || 24 * 60 * 60
    this.streams = []

    // binding
    this.genKeySeri = this.genKeySeri.bind(this)
  }

  /**
   * Generate seri key by name, seriNumber
   * @param  {Number} seriNumber
   * @return {string}
   */
  genKeySeri(seriNumber) {
    return `queue_${this.name}_store_${seriNumber}`
  }

  /**
   * Generate stream key by name
   * @param  {string} name
   * @return {string}
   */
  genKeyStream(name) {
    return `queue_${this.name}_${name}`
  }

  /**
   * Set redis client for all partiton
   */
  setRedisClient(redisClient) {
    this.redisClient = redisClient
    this.streams.forEach(stream => {
      stream.redisClient = redisClient
      stream.notify()
    })
  }

  /**
   * Create and add stream
   * @param {Stream} stream
   */
  newStream(name, type, options) {

    const stream = new Stream(
      name,
      type,
      {
        keyStream: this.genKeyStream(name),
        redisClient: this.redisClient,
        genKeySeri: this.genKeySeri,
        ...options || {}
      }
    )

    this.streams.push(stream)
    return this
  }

  /**
   * Create message and add default options
   * @return {string}
   */
  messageCreator(message, id) {

    return JSON.stringify({
      id,
      ...message,
      // default options
      timestamp: message.timestamp || + new Date()
    })
  }

  /**
   * Store message and push to all streams
   * @param  {Object} message
   */
  push(message) {
    this.redisClient.incr(QUEUE_SERI_NUMBER, (err, reply) => {
      if (err) throw err
      let seriNumber = 0
      if (reply) seriNumber = +reply

      // gen multi commands
      let commandChain = []

      // reset seriNumber if it's not safe
      if (seriNumber >= Number.MAX_SAFE_INTEGER) commandChain.push(['del', QUEUE_SERI_NUMBER])

      // store message
      commandChain.push([
        'SET',
        this.genKeySeri(seriNumber),
        this.messageCreator(message, seriNumber),
        'EX',
        this.expires
      ])

      // push message to streams
      this.streams.forEach(stream => {
        if (!stream.messageType || stream.messageType === message.type) {
          commandChain.push([
            'RPUSH',
            this.genKeyStream(stream.name),
            seriNumber
          ])
        }
      })

      // execute commandChain
      this.redisClient.multi(commandChain).exec((err, reply) => {
        if (err) throw err
        // console.log(reply)
        this.notify()
      })
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

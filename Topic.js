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
      expires
    } = options || {}

    this.redisClient = null
    this.name = name
    this.expires = expires || 24 * 60 * 60
    this.streams = []
  }

  /**
   * Generate seri key by name, seriNumber
   * @param  {Number} seriNumber
   * @return {String}
   */
  genKeySeri(seriNumber) {
    return `queue_${this.name}_store_${seriNumber}`
  }

  /**
   * Generate stream key by name
   * @param  {String} name
   * @return {String}
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
   * Add stream to topic
   * @param {Stream} stream
   */
  addStream(stream) {
    if (!(stream instanceof Stream)) throw new Error('Param stream must be instance of Stream')
    this.streams.push(stream)

    // set required property for stream
    stream.keyStream = this.genKeyStream(stream.name)
    stream.genKeySeri = this.genKeySeri.bind(this)
    if (this.redisClient) stream.redisClient = this.redisClient
  }

  /**
   * Create message and add default options
   * @return {String}
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
    this.redisClient.get(QUEUE_SERI_NUMBER, (err, reply) => {
      if (err) throw err
      let seriNumber = 0
      if (reply) seriNumber = +reply
      // reset seriNumber if it's not safe
      if (seriNumber >= Number.MAX_SAFE_INTEGER) seriNumber = 0

      // gen multi commands
      let commandChain = []

      // incre seriNumber
      commandChain.push(["incr", QUEUE_SERI_NUMBER])

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
            'LPUSH',
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

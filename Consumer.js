/**
 * Consumer subscribe to topic for get message
 */

class Consumer {
  constructor() {
    this.handle = (message, finish) => {
      console.log('consumer not regist onMessage')
      finish()
    }
    this.messageType = ''
  }

  subscribe(topic, options) {
    if (!Array.isArray(topic.subscribers)) throw Error('Param topic must be instance of Topic')
    topic.subscribers.push(this)
  }

  /**
   * Regist handle
   * @param  {string}   messageType if null, '', lost -> apply for all message
   * @param  {Function} callback    callback(message, finish)
   */
  onMessage(messageType, callback) {
    const cb = (typeof messageType === 'function')? messageType : callback
    this.messageType = (typeof messageType === 'string')? messageType : ''
    this.handle = cb
  }
}

module.exports = Consumer

/**
 * @class IEx
 */

class IEx {
  constructor() {
    // store all events of queue
    this.events = {}

    this.on = this.on.bind(this)
    this.emit = this.emit.bind(this)
  }

  /**
   * Listen a evnet
   * @param  {string} event  event name
   * @param  {Function} handle event's hangle
   */
  on(event, handle) {
    if (typeof handle !== 'function') throw new Error('handle must be a function')
    this.events[event] = handle
  }

  /**
   * Trigger a event
   * @param  {string} event  event name
   * @param  {Mixe} data  data pass to event handle
   */
  emit(event, data) {
    if (typeof this.events[event] === 'function') this.events[event](data)
  }
}

module.exports = IEx
const RedisQueue = require('./')
const queue = new RedisQueue()

queue.clean()

queue.addEvent(
  'click',
  function log(data, next, reject) {
    console.log('log data = ' + JSON.stringify(data))
    next()
  },
  function bu(data, next, reject) {
    setTimeout(() => {
      console.log('backup data = ' + JSON.stringify(data))
      next()
      if (data.bundle === 'shhhh 2') reject()
    }, 1000)
  }
)

queue.push('click', { bundle: 'shhhh 1' }, (err) => {
  if (err) console.log(err)
})
// queue.push('click', { bundle: 'shhhh 2' })

// queue.notify()
// queue.shutdown()

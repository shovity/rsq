# Redis stream queue

Install with:

  npm install rsq

## Usage Example

```js
const Queue = require('rsq')
const queue = new Queue()

// streams working concurently
queue.newTopic('log').newStream('mysql').newStream('otherStream')

queue.registHandle(
  [
    { topic: 'log', stream: 'mysql', type: 'create' },
    // { topic: 'log', stream: ['mysql', 'outherStream'], type: ['create', 'otherType'] },
  ],
  (message, done) => {
    console.log('LOG CREATE: ' + JSON.stringify(message))
    done()
  }
)

queue.registHandle(
  [
    { topic: 'log', stream: 'mysql', type: 'remove' },
  ],
  (message, done) => {
    console.log('LOG REMOVE: ' + JSON.stringify(message))
    done()
  }
)

setInterval(() => {
  queue.push({
    topic: 'log',
    stream: 'mysql',
    type: (Math.random() < 0.5)? 'create' : 'remove',
    payload: { something: 'data' }
  })
}, 100)
```

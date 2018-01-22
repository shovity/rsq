nodejs - redis stream queue
===========================

[![NPM version](https://badge.fury.io/js/rsq.svg)](https://www.npmjs.com/package/rsq)
![Downloads](https://img.shields.io/npm/dm/rsq.svg?style=flat)

## Installation
```sh
# Get the latest stable release of rsq
$ npm install rsq
or
$ yarn install rsq
```

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

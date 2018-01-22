nodejs - redis stream queue
===========================

[![Build Status](https://travis-ci.org/NodeRedis/node_redis.svg?branch=master)](https://travis-ci.org/NodeRedis/node_redis)
[![Coverage Status](https://coveralls.io/repos/NodeRedis/node_redis/badge.svg?branch=)](https://coveralls.io/r/NodeRedis/node_redis?branch=)
[![Windows Tests](https://img.shields.io/appveyor/ci/BridgeAR/node-redis/master.svg?label=Windows%20Tests)](https://ci.appveyor.com/project/BridgeAR/node-redis/branch/master)
[![Gitter](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/NodeRedis/node_redis?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge)

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

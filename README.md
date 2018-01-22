### Nodejs - redis stream queue

Stream queue using [redis](https://redis.io/) with driver [node_redis](https://github.com/NodeRedis/node_redis)

[![NPM version](https://badge.fury.io/js/rsq.svg)](https://www.npmjs.com/package/rsq)
![Downloads](https://img.shields.io/npm/dm/rsq.svg?style=flat)

### Installation
```sh
# Get the latest stable release of rsq
$ npm install rsq
  or
$ yarn install rsq
```

### Usage Example

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
    setTimeout(() => {
      console.log('LOG CREATE: ' + JSON.stringify(message))
      done()
    }, 200)
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

for (let i = 0; i < 10; i++) {
  queue.push({
    topic: 'log',
    stream: 'mysql',
    type: (Math.random() < 0.5)? 'create' : 'remove',
    payload: { data: 'something' + i }
  })
}
```

### APIs
```js
const queue = new Queue([name], [config])
const topic = queue.newTopic(name, [config]).newStream(name, [config])
```

### options object properties
| Property | Default | Description |
|----------|---------|-------------|
| redisClient | redis client | Client node redis, default create with redisConfig |
| redisConfig | null | Default options redis client |
| priority | 0 | 0-timeout, 1-imediate, 2-nextTick |
| expires | 2 day | Expires key redis (secends) |

see redisConfig [node_redis](https://github.com/NodeRedis/node_redis)

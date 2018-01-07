const redis = require("redis")
const sub = redis.createClient()
const pub = redis.createClient()

sub.on("subscribe", (channel, count) => {
  pub.publish("channel 1", "I am sending a message.")
  pub.publish("channel 1", "I am sending a second message.")
  pub.publish("channel 1", "I am sending my last message.")
})

sub.on("message", (channel, message) => {
  console.log("Channel: " + channel + " - Message: " + message)
})

sub.subscribe("channel 1")

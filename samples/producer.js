var redis = require('redis');
var ReliableQueue = require('../index');
var Promise = require('bluebird');

var r = new ReliableQueue(redis.createClient());
//r.ttl = 600;

var counter = 0;
function produceTask() {

  r.enqueue('fisclet2', 'data:' + (counter++))
    .then(function(key){
      console.log('Enqueue with key: ' + key);
      Promise.delay(500).then(produceTask);
    }).catch(function(err){
      console.log(err);
      Promise.delay(500).then(produceTask);
    })
};

produceTask();

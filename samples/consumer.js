var redis = require('redis');
var ReliableQueue = require('../index');
var Promise = require('bluebird');

var r = new ReliableQueue(redis.createClient());
//r.ttl = 600;
var consumeDelay = 10;
var processTask = function () {
  let task;
  let completed;

  r.dequeue('fisclet2')
    .then(function(task){
      console.log('task comming');
      console.log('key: ' + task[0]);
      console.log('data: ' + task[1]);
      return r.complete('fisclet2', task[0]);
    }).then(function(completed){
      console.log('removed ' + completed);
      Promise.delay(consumeDelay).then(processTask);
    }).catch(function(err){
      if (err.code != 'NORESULT') {
        console.log(err);
      }
      Promise.delay(consumeDelay).then(processTask);
    })


};

processTask();

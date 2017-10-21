var redis = require('redis');
var ReliableQueue = require('../index');
var Promise = require('bluebird');

var r = new ReliableQueue(redis.createClient());
//r.ttl = 600;

var counter = 0;
var multiAmount = 1;
var delay = 0;
var maxInsertAmount = 1000;
var startTime = new Date();
function produceTask() {
  var data = [];
  for(var i = 0; i < multiAmount; i++){
	  data.push(
		  'data:' + (counter++)
	  )
  }

  r.enqueueMulti('fisclet2', data)
    .then(function(keys){
	  //console.log('Enqueue with keys: ' + keys);
	  if(counter >= maxInsertAmount && maxInsertAmount > 0)
	  {
		console.log("duration:" + ((new Date() - startTime)));
		return;
	  }
      Promise.delay(delay).then(produceTask);
    }).catch(function(err){
      console.log(err);
      Promise.delay(delay).then(produceTask);
    })
};

produceTask();

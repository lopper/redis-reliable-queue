var Promise = require('bluebird');
var redis = require('redis');
var luaScripts   = require('./redis-queue-lua');
var NoResultError = require('./noresult-error');

Promise.promisifyAll(redis.RedisClient.prototype);
Promise.promisifyAll(redis.Multi.prototype);
var ReliableQueue = class ReliableQueue {
  constructor(redis) {
    if (!redis) {
      throw new TypeError('reliable required RedisClient instance');
    }

    this.redisdb = redis;
    this.expires = 600;
  }

  executeAsync (command, args) {
    args.unshift(0);
    let ret;

    var self = this;
    return this.redisdb.evalshaAsync(luaScripts.LUA_SHA[command], args)
      .then(function(ret){
         if(!ret){
            throw new NoResultError('No result for ' + command, 'NORESULT');
         }

         return ret;
      }).catch(function(err){
        if (err.code == luaScripts.NOSCRIPT) {
          return self.redisdb.evalAsync(luaScripts.LUA_SCRIPTS[command], args);
        }
        throw err;
      })

  }

  set ttl(expires) {
    this.expires = expires;
  }
  /* 
    dataList : [
      data
    ]
  */
  enqueueMulti(qname, datalist){
    let ret;

    var self = this;
    var multi = this.redisdb.multi();
    for(var i = 0; i < datalist.length; i++){
      var data = [0, qname, datalist[i]];
      multi.evalAsync(luaScripts.LUA_SCRIPTS['enqueue'], data);
    }
    return multi.execAsync()
      .then(function(replies){
        return replies;
      }).catch(function(err){

        throw err;
      })
  }
  enqueue (qname, data) {
    return this.executeAsync('enqueue', [qname, data]);
  }

  dequeue (qname, cb) {
    return this.executeAsync('dequeue', [qname, this.expires]);
  };

  complete (qname, key) {
    // Delete from processing on complete
    return this.redisdb.lremAsync(qname + ':processing', 0, key);
  }

};

module.exports = ReliableQueue;

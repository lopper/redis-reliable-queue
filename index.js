var Promise = require('bluebird');
var redis = require('redis');
var luaScripts   = require('./redis-queue-lua');
var NoResultError = require('./noresult-error');
var _ = require('underscore');

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

  getNumQueued(qname){
    return this.redisdb.llenAsync(qname + ':queued')
  }

  getNumProcessing(qname){
    return this.redisdb.llenAsync(qname + ':processing')
  }

  getAllData(qname){
    return this.redisdb.hvalsAsync(qname + ':data')
  }

  requeueExpired (qname , max_inspect) {
    max_inspect = max_inspect || 500;
		var self = this;
		var data = {
			keys : [],
			processing_ids : []
		}

		var PROCESSING_QUEUE_NAME =  qname + ':processing';
		var PENDING_QUEUE_NAME =  qname + ':queued';
		
		this.redisdb.lrangeAsync(PROCESSING_QUEUE_NAME, 0, max_inspect)
			.then(function(ids){
				// create list of keys
				data.processing_ids = _.uniq(ids);
				_.each(data.processing_ids, function(id){
					data.keys.push(`${qname}:tracking:${id}`);
				})

				if(data.keys.length == 0)
					return [];

				return self.redisdb.mgetAsync(data.keys);
			}).then(function(items /* list */){
				if(items.length === 0)
					return [];

				var keysToTransfer = [];
				_.each(items, function(item, idx){
					if(item == null)
						keysToTransfer.push(data.processing_ids[idx])
				});

				var multi = self.redisdb.multi();
				multi.rpushAsync(PENDING_QUEUE_NAME, keysToTransfer)
				
				_.each(keysToTransfer, function(key, idx){
					multi.lremAsync(PROCESSING_QUEUE_NAME, 0 , key);
				})
				
				return multi.execAsync().catch(function(err){
				  throw err;
				})

			}).then(function(replies){
        // num transfered
        return {
            numRequeued : data.keys.length
        }
			})
  }

  complete (qname, key) {
    // Delete from processing on complete
    return this.redisdb.lremAsync(qname + ':processing', 0, key);
  }

};

module.exports = ReliableQueue;

var Fiber = Npm.require('fibers');
var publicationPerformance = Npm.require('perf_hooks').performance;
var publicationTimings = new WeakMap();
var activePublications = new Set();
var recentPublications = Kadira._createActivityHistory();

Kadira.getPublicationActivity = function(start, end) {
  return recentPublications.snapshot(activePublications, start, end, function(record, now) {
    return {
      publication: record.name,
      status: record.endedAt !== undefined ? record.status : record.handlerReturned ? 'waitingForReady' : 'runningHandler',
      startedAt: new Date(publicationPerformance.timeOrigin + record.startedAt).toISOString(),
      endedAt: record.endedAt === undefined ? undefined : new Date(publicationPerformance.timeOrigin + record.endedAt).toISOString(),
      elapsedMs: Math.round(now - record.startedAt)
    };
  });
};

function finishPublicationTiming(subscription, status) {
  var record = publicationTimings.get(subscription);
  if (!record) return;
  if (status) record.status = status;
  if (!record.status || !record.handlerReturned) return;
  publicationTimings.delete(subscription);
  activePublications.delete(record);
  record.endedAt = publicationPerformance.now();
  recentPublications.add(record);
}

wrapSubscription = function(subscriptionProto) {
  // If the ready event runs outside the Fiber, Kadira._getInfo() doesn't work.
  // we need some other way to store kadiraInfo so we can use it at ready hijack.
  var originalRunHandler = subscriptionProto._runHandler;
  subscriptionProto._runHandler = function() {
    var kadiraInfo = Kadira._getInfo();
    if (kadiraInfo) {
      this.__kadiraInfo = kadiraInfo;
    };
    var record = { name: this._name || '<anonymous>', startedAt: publicationPerformance.now(), handlerReturned: false };
    publicationTimings.set(this, record);
    activePublications.add(record);
    try {
      return originalRunHandler.apply(this, arguments);
    } catch (error) {
      record.status = 'failed';
      throw error;
    } finally {
      record.handlerReturned = true;
      finishPublicationTiming(this);
    }
  }

  var originalReady = subscriptionProto.ready;
  subscriptionProto.ready = function() {
    // meteor has a field called `_ready` which tracks this
    // but we need to make it future proof
    if(!this._apmReadyTracked) {
      var kadiraInfo = Kadira._getInfo() || this.__kadiraInfo;
      delete this.__kadiraInfo;
      //sometime .ready can be called in the context of the method
      //then we have some problems, that's why we are checking this
      //eg:- Accounts.createUser
      if(kadiraInfo && this._subscriptionId == kadiraInfo.trace.id) {
        var isForced = Kadira.tracer.endLastEvent(kadiraInfo.trace);
        if (isForced) {
          console.warn('Kadira endevent forced complete', JSON.stringify(kadiraInfo.trace.events));
        };
        Kadira.tracer.event(kadiraInfo.trace, 'complete');
        var trace = Kadira.tracer.buildTrace(kadiraInfo.trace);
      }

      Kadira.EventBus.emit('pubsub', 'subCompleted', trace, this._session, this);
      Kadira.models.pubsub._trackReady(this._session, this, trace);
      this._apmReadyTracked = true;
    }

    // we still pass the control to the original implementation
    // since multiple ready calls are handled by itself
    try {
      return originalReady.call(this);
    } finally {
      finishPublicationTiming(this, 'ready');
    }
  };

  var originalError = subscriptionProto.error;
  subscriptionProto.error = function(err) {
    var kadiraInfo = Kadira._getInfo();

    if(kadiraInfo && this._subscriptionId == kadiraInfo.trace.id) {
      Kadira.tracer.endLastEvent(kadiraInfo.trace);

      var errorForApm = _.pick(err, 'message', 'stack');
      Kadira.tracer.event(kadiraInfo.trace, 'error', {error: errorForApm});
      var trace = Kadira.tracer.buildTrace(kadiraInfo.trace);

      Kadira.models.pubsub._trackError(this._session, this, trace);

      // error tracking can be disabled and if there is a trace
      // trace should be avaialble all the time, but it won't
      // if something wrong happened on the trace building
      if(Kadira.options.enableErrorTracking && trace) {
        Kadira.models.error.trackError(err, trace);
      }
    }

    // wrap error stack so Meteor._debug can identify and ignore it
    err.stack = {stack: err.stack, source: 'subscription'};
    originalError.call(this, err);
  };

  var originalDeactivate = subscriptionProto._deactivate;
  subscriptionProto._deactivate = function() {
    Kadira.EventBus.emit('pubsub', 'subDeactivated', this._session, this);
    Kadira.models.pubsub._trackUnsub(this._session, this);
    try {
      return originalDeactivate.call(this);
    } finally {
      finishPublicationTiming(this, 'stopped');
    }
  };

  //adding the currenSub env variable
  ['added', 'changed', 'removed'].forEach(function(funcName) {
    var originalFunc = subscriptionProto[funcName];
    subscriptionProto[funcName] = function(collectionName, id, fields) {
      var self = this;

      // we need to run this code in a fiber and that's how we track
      // subscription info. May be we can figure out, some other way to do this
      // We use this currently to get the publication info when tracking message
      // sizes at wrap_ddp_stringify.js
      Kadira.env.currentSub = self;
      var res = originalFunc.call(self, collectionName, id, fields);
      Kadira.env.currentSub = null;

      return res;
    };
  });
};

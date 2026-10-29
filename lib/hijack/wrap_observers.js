var observerPerformance = Npm.require('perf_hooks').performance;
var observerOwners = new WeakMap();
var slowObservers = [];

function getObserverDocSize(driver, data) {
  var description = driver._cursorDescription;
  // An observer's cursor description stays fixed for its lifetime.
  if (driver._kadiraDocSizeCacheKey === undefined) {
    driver._kadiraDocSizeCacheKey = Kadira.docSzCache.getKey(
      description.collectionName, description.selector, description.options);
    driver._kadiraDocSizeCacheKeyBytes = Buffer.byteLength(driver._kadiraDocSizeCacheKey, 'utf8');
  }
  return Kadira.docSzCache.getSize(description.collectionName, description.selector,
    description.options, data, driver._kadiraDocSizeCacheKey);
}

function observerResultCount(results) {
  if (!results) return 0;
  return typeof results.size === 'function' ? results.size() : results.length || 0;
}

function recordSlowObserver(driver, operation, driverType, startedAt) {
  try {
    var elapsedMs = observerPerformance.now() - startedAt;
    if (elapsedMs < 50) return;

    var description = driver._cursorDescription;
    var owner = driver._ownerInfo || (driver._kadiraObserverOwner !== undefined
      ? driver._kadiraObserverOwner : observerOwners.get(description));
    var options = description.options || {};
    slowObservers.push({
      publication: owner && owner.type === 'sub' ? owner.name : null,
      ownerType: owner ? owner.type : null,
      ownerName: owner ? owner.name : null,
      collection: description.collectionName,
      operation: operation,
      driver: driverType,
      elapsedMs: Math.round(elapsedMs),
      startedAt: new Date(observerPerformance.timeOrigin + startedAt).toISOString(),
      endedAt: new Date(observerPerformance.timeOrigin + startedAt + elapsedMs).toISOString(),
      documentCount: driver._published || driver._results
        ? observerResultCount(driver._published || driver._results) : null,
      bufferDocumentCount: observerResultCount(driver._unpublishedBuffer),
      queryKeyBytes: driver._kadiraDocSizeCacheKeyBytes === undefined ? null : driver._kadiraDocSizeCacheKeyBytes,
      projectedFieldCount: Object.keys(options.fields || options.projection || {}).length,
    });
    // Keep only the slowest operations until the event-loop monitor collects them.
    slowObservers.sort(function(a, b) { return b.elapsedMs - a.elapsedMs; });
    slowObservers.length = Math.min(slowObservers.length, 20);
  } catch (error) {
    // Diagnostics must not change the observer's result or hide its error.
  }
}

Kadira.takeObserverDiagnostics = function() {
  var records = slowObservers;
  slowObservers = [];
  return records;
};

function timeObserverOperation(proto, operation, driverType) {
  var original = proto[operation];
  proto[operation] = function() {
    var startedAt = observerPerformance.now();
    try {
      return original.apply(this, arguments);
    } finally {
      recordSlowObserver(this, operation, driverType, startedAt);
    }
  };
}

wrapOplogObserveDriver = function(proto) {
  // Track the polled documents. This is reflect to the RAM size and
  // for the CPU usage directly
  var originalPublishNewResults = proto._publishNewResults;
  proto._publishNewResults = function(newResults, newBuffer) {
    var docSize = getObserverDocSize(this, newResults);
    var bufferDocSize = getObserverDocSize(this, newBuffer);
    var count = newResults.size() + newBuffer.size();
    var totalSize = docSize * newResults.size() + bufferDocSize * newBuffer.size();
    if(this._ownerInfo) {
      Kadira.models.pubsub.trackPolledDocuments(this._ownerInfo, count);
      Kadira.models.pubsub.trackDocSize(this._ownerInfo.name, "polledFetches", totalSize);
    } else {
      this._polledDocuments = count;
      this._docSize = {
        polledFetches: totalSize
      }
    }
    return originalPublishNewResults.call(this, newResults, newBuffer);
  };

  var originalHandleOplogEntryQuerying = proto._handleOplogEntryQuerying;
  proto._handleOplogEntryQuerying = function(op) {
    Kadira.models.pubsub.trackDocumentChanges(this._ownerInfo, op);
    return originalHandleOplogEntryQuerying.call(this, op);
  };

  var originalHandleOplogEntrySteadyOrFetching = proto._handleOplogEntrySteadyOrFetching;
  proto._handleOplogEntrySteadyOrFetching = function(op) {
    Kadira.models.pubsub.trackDocumentChanges(this._ownerInfo, op);
    return originalHandleOplogEntrySteadyOrFetching.call(this, op);
  };

  // track live updates
  ['_addPublished', '_removePublished', '_changePublished'].forEach(function(fnName) {
    var originalFn = proto[fnName];
    proto[fnName] = function(a, b, c) {
      if(this._ownerInfo) {
        Kadira.models.pubsub.trackLiveUpdates(this._ownerInfo, fnName, 1);

        if(fnName === "_addPublished") {
          var docSize = getObserverDocSize(this, [b]);

          Kadira.models.pubsub.trackDocSize(this._ownerInfo.name, "liveFetches", docSize);
        }
      } else {
        // If there is no ownerInfo, that means this is the initial adds
        if(!this._liveUpdatesCounts) {
          this._liveUpdatesCounts = {
            _initialAdds: 0
          };
        }

        this._liveUpdatesCounts._initialAdds++;

        if(fnName === "_addPublished") {
          if(!this._docSize) {
            this._docSize = {
              initialFetches: 0
            };
          }

          if(!this._docSize.initialFetches) {
            this._docSize.initialFetches = 0;
          }

          var docSize = getObserverDocSize(this, [b]);

          this._docSize.initialFetches += docSize;
        }
      }

      return originalFn.call(this, a, b, c);
    };
  });

  var originalStop = proto.stop;
  proto.stop = function() {
    if(this._ownerInfo && this._ownerInfo.type === 'sub') {
      Kadira.EventBus.emit('pubsub', 'observerDeleted', this._ownerInfo);
      Kadira.models.pubsub.trackDeletedObserver(this._ownerInfo);
    }

    return originalStop.call(this);
  };

  ['_publishNewResults', '_handleOplogEntryQuerying', '_handleOplogEntrySteadyOrFetching'].forEach(function(operation) {
    timeObserverOperation(proto, operation, 'oplog');
  });
};

wrapPollingObserveDriver = function(proto) {
  var originalPollMongo = proto._pollMongo;
  proto._pollMongo = function() {
    originalPollMongo.call(this);

    // Current result is stored in the following variable.
    // So, we can use that
    // Sometimes, it's possible to get size as undefined.
    // May be something with different version. We don't need to worry about
    // this now
    var count = 0;
    var docSize = 0;

    if(this._results) {
      count = observerResultCount(this._results);
      docSize = getObserverDocSize(this, this._results)*count;
    }

    if(this._ownerInfo) {
      Kadira.models.pubsub.trackPolledDocuments(this._ownerInfo, count);
      Kadira.models.pubsub.trackDocSize(this._ownerInfo.name, "polledFetches", docSize);
    } else {
      this._polledDocuments = count;
      this._polledDocSize = docSize;
    }
  };

  var originalStop = proto.stop;
  proto.stop = function() {
    if(this._ownerInfo && this._ownerInfo.type === 'sub') {
      Kadira.EventBus.emit('pubsub', 'observerDeleted', this._ownerInfo);
      Kadira.models.pubsub.trackDeletedObserver(this._ownerInfo);
    }

    return originalStop.call(this);
  };

  timeObserverOperation(proto, '_pollMongo', 'polling');
};

wrapMultiplexer = function(proto) {
  var originalInitalAdd = proto.addHandleAndSendInitialAdds;
   proto.addHandleAndSendInitialAdds = function(handle) {
    if(!this._firstInitialAddTime) {
      this._firstInitialAddTime = Date.now();
    }

    handle._wasMultiplexerReady = this._ready();
    handle._queueLength = this._queue._taskHandles.length;

    if(!handle._wasMultiplexerReady) {
      handle._elapsedPollingTime = Date.now() - this._firstInitialAddTime;
    }
    return originalInitalAdd.call(this, handle);
  };
};

wrapForCountingObservers = function() {
  // to count observers
  var mongoConnectionProto = MeteorX.MongoConnection.prototype;
  var originalObserveChanges = mongoConnectionProto._observeChanges;
  mongoConnectionProto._observeChanges = function(cursorDescription, ordered, callbacks) {
    // get the Kadira Info via the Meteor.EnvironmentalVariable
    var kadiraInfo = Kadira._getInfo(null, true);
    var publication = DDP._CurrentPublicationInvocation.get();
    var owner = publication ? { type: 'sub', name: publication._name || '<anonymous>' }
      : kadiraInfo ? { type: kadiraInfo.trace.type, name: kadiraInfo.trace.name } : null;
    if (owner) observerOwners.set(cursorDescription, owner);
    else observerOwners.delete(cursorDescription);
    var startedAt = observerPerformance.now();
    var ret;
    try {
      ret = originalObserveChanges.apply(this, arguments);
    } finally {
      var driver = ret && ret._multiplexer && ret._multiplexer._observeDriver;
      if (driver && driver._kadiraObserverOwner === undefined) driver._kadiraObserverOwner = owner;
      recordSlowObserver(driver || { _cursorDescription: cursorDescription },
        '_observeChanges', 'setup', startedAt);
    }

    if(kadiraInfo && ret._multiplexer) {
      if(!ret._multiplexer.__kadiraTracked) {
        // new multiplexer
        ret._multiplexer.__kadiraTracked = true;
        Kadira.EventBus.emit('pubsub', 'newSubHandleCreated', kadiraInfo.trace);
        Kadira.models.pubsub.incrementHandleCount(kadiraInfo.trace, false);
        if(kadiraInfo.trace.type == 'sub') {
          var ownerInfo = {
            type: kadiraInfo.trace.type,
            name: kadiraInfo.trace.name,
            startTime: (new Date()).getTime()
          };

          var observerDriver = ret._multiplexer._observeDriver;
          observerDriver._ownerInfo = ownerInfo;
          Kadira.EventBus.emit('pubsub', 'observerCreated', ownerInfo);
          Kadira.models.pubsub.trackCreatedObserver(ownerInfo);

          // We need to send initially polled documents if there are
          if(observerDriver._polledDocuments) {
            Kadira.models.pubsub.trackPolledDocuments(ownerInfo, observerDriver._polledDocuments);
            observerDriver._polledDocuments = 0;
          }

          // We need to send initially polled documents if there are
          if(observerDriver._polledDocSize) {
            Kadira.models.pubsub.trackDocSize(ownerInfo.name, "polledFetches", observerDriver._polledDocSize);
            observerDriver._polledDocSize = 0;
          }

          // Process _liveUpdatesCounts
          _.each(observerDriver._liveUpdatesCounts, function(count, key) {
            Kadira.models.pubsub.trackLiveUpdates(ownerInfo, key, count);
          });

          // Process docSize
          _.each(observerDriver._docSize, function(count, key) {
            Kadira.models.pubsub.trackDocSize(ownerInfo.name, key, count);
          });
        }
      } else {
        Kadira.EventBus.emit('pubsub', 'cachedSubHandleCreated', kadiraInfo.trace);
        Kadira.models.pubsub.incrementHandleCount(kadiraInfo.trace, true);
      }
    }

    return ret;
  }
};

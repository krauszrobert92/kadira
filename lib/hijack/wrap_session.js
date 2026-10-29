var methodPerformance = Npm.require('perf_hooks').performance;
var activeMethods = new Set();
var slowMethods = [];
var wrappedMethodHandlers = new WeakMap();

function methodTiming(record, now) {
  return {
    method: record.name,
    startedAt: new Date(methodPerformance.timeOrigin + record.startedAt).toISOString(),
    elapsedMs: Math.round(now - record.startedAt)
  };
}

function finishMethodTiming(record, succeeded) {
  activeMethods.delete(record);
  try {
    var now = methodPerformance.now();
    if (now - record.startedAt < 50) return;
    var timing = methodTiming(record, now);
    timing.endedAt = new Date(methodPerformance.timeOrigin + now).toISOString();
    timing.succeeded = succeeded;
    slowMethods.push(timing);
    slowMethods.sort(function(a, b) { return b.elapsedMs - a.elapsedMs; });
    slowMethods.length = Math.min(slowMethods.length, 20);
  } catch (error) {
    // Diagnostics must preserve the method's result or error.
  }
}

Kadira.takeMethodDiagnostics = function() {
  var completed = slowMethods;
  slowMethods = [];
  var running = [];
  var now = methodPerformance.now();
  activeMethods.forEach(function(record) {
    if (running.length < 20) running.push(methodTiming(record, now));
  });
  return { slowMethods: completed, activeMethods: running, activeMethodCount: activeMethods.size };
};

function trackMethodTiming(name, handler) {
  return function() {
    var record = { name: name, startedAt: methodPerformance.now() };
    activeMethods.add(record);
    try {
      var result = handler.apply(this, arguments);
      if (result && typeof result.then === 'function') {
        return Promise.resolve(result).then(function(value) {
          finishMethodTiming(record, true);
          return value;
        }, function(error) {
          finishMethodTiming(record, false);
          throw error;
        });
      }
      finishMethodTiming(record, true);
      return result;
    } catch (error) {
      finishMethodTiming(record, false);
      throw error;
    }
  };
}

wrapSession = function (sessionProto) {
  var originalProcessMessage = sessionProto.processMessage;
  sessionProto.processMessage = function (msg) {
    if (true) {
      var kadiraInfo = {
        session: this.id,
        userId: this.userId
      };

      if (msg.msg == 'method' || msg.msg == 'sub') {
        kadiraInfo.trace = Kadira.tracer.start(this, msg);
        Kadira.waitTimeBuilder.register(this, msg.id);

        //use JSON stringify to save the CPU
        var startData = { userId: this.userId, params: JSON.stringify(msg.params) };
        Kadira.tracer.event(kadiraInfo.trace, 'start', startData);
        var waitEventId = Kadira.tracer.event(kadiraInfo.trace, 'wait', {}, kadiraInfo);
        msg._waitEventId = waitEventId;
        msg.__kadiraInfo = kadiraInfo;

        if (msg.msg == 'sub') {
          // start tracking inside processMessage allows us to indicate
          // wait time as well
          Kadira.EventBus.emit('pubsub', 'subReceived', this, msg);
          Kadira.models.pubsub._trackSub(this, msg);
        }
      }

      // Update session last active time
      Kadira.EventBus.emit('system', 'ddpMessageReceived', this, msg);
      Kadira.models.system.handleSessionActivity(msg, this);
    }

    return originalProcessMessage.call(this, msg);
  };

  //adding the method context to the current fiber
  var originalMethodHandler = sessionProto.protocol_handlers.method;
  sessionProto.protocol_handlers.method = function (msg, unblock) {
    var self = this;
    //add context
    var kadiraInfo = msg.__kadiraInfo;
    if (kadiraInfo) {
      Kadira._setInfo(kadiraInfo);

      // end wait event
      var waitList = Kadira.waitTimeBuilder.build(this, msg.id);
      Kadira.tracer.eventEnd(kadiraInfo.trace, msg._waitEventId, { waitOn: waitList });

      unblock = Kadira.waitTimeBuilder.trackWaitTime(this, msg, unblock);
      var response = Kadira.env.kadiraInfo.withValue(kadiraInfo, function () {
        return originalMethodHandler.call(self, msg, unblock);
      });
      unblock();
    } else {
      var response = originalMethodHandler.call(self, msg, unblock);
    }

    return response;
  };

  //to capture the currently processing message
  var orginalSubHandler = sessionProto.protocol_handlers.sub;
  sessionProto.protocol_handlers.sub = function (msg, unblock) {
    var self = this;
    //add context
    var kadiraInfo = msg.__kadiraInfo;
    if (kadiraInfo) {
      Kadira._setInfo(kadiraInfo);

      // end wait event
      var waitList = Kadira.waitTimeBuilder.build(this, msg.id);
      Kadira.tracer.eventEnd(kadiraInfo.trace, msg._waitEventId, { waitOn: waitList });

      unblock = Kadira.waitTimeBuilder.trackWaitTime(this, msg, unblock);
      var response = Kadira.env.kadiraInfo.withValue(kadiraInfo, function () {
        return orginalSubHandler.call(self, msg, unblock);
      });
      unblock();
    } else {
      var response = orginalSubHandler.call(self, msg, unblock);
    }

    return response;
  };

  //to capture the currently processing message
  var orginalUnSubHandler = sessionProto.protocol_handlers.unsub;
  sessionProto.protocol_handlers.unsub = function (msg, unblock) {
    unblock = Kadira.waitTimeBuilder.trackWaitTime(this, msg, unblock);
    var response = orginalUnSubHandler.call(this, msg, unblock);
    unblock();
    return response;
  };

  //track method ending (to get the result of error)
  var originalSend = sessionProto.send;
  sessionProto.send = function (msg) {
    if (msg.msg == 'result') {
      var kadiraInfo = Kadira._getInfo();
      if (kadiraInfo) {
        if (msg.error) {
          var error = _.pick(msg.error, ['message', 'stack']);

          // pick the error from the wrapped method handler
          if (kadiraInfo && kadiraInfo.currentError) {
            // the error stack is wrapped so Meteor._debug can identify
            // this as a method error.
            error = _.pick(kadiraInfo.currentError, ['message', 'stack']);
            // see wrapMethodHanderForErrors() method def for more info
            if (error.stack && error.stack.stack) {
              error.stack = error.stack.stack;
            }
          }

          Kadira.tracer.endLastEvent(kadiraInfo.trace);
          Kadira.tracer.event(kadiraInfo.trace, 'error', { error: error });
        } else {
          var isForced = Kadira.tracer.endLastEvent(kadiraInfo.trace);
          if (isForced) {
            console.warn('Kadira endevent forced complete', JSON.stringify(kadiraInfo.trace.events));
          };
          Kadira.tracer.event(kadiraInfo.trace, 'complete');
        }

        //processing the message
        var trace = Kadira.tracer.buildTrace(kadiraInfo.trace);
        Kadira.EventBus.emit('method', 'methodCompleted', trace, this);
        Kadira.models.methods.processMethod(trace);

        // error may or may not exist and error tracking can be disabled
        if (error && Kadira.options.enableErrorTracking) {
          Kadira.models.error.trackError(error, trace);
        }

        //clean and make sure, fiber is clean
        //not sure we need to do this, but a preventive measure
        Kadira._setInfo(null);
      }
    }

    return originalSend.call(this, msg);
  };
};

// wrap existing method handlers for capturing errors
_.each(Meteor.server.method_handlers, function (handler, name) {
  wrapMethodHanderForErrors(name, handler, Meteor.server.method_handlers);
});

// REST captures the server registrar and replaces Meteor.methods, so wrap that registrar.
var methodServerProto = Object.getPrototypeOf(Meteor.server);
var originalServerMethods = methodServerProto.methods;
methodServerProto.methods = function (methodMap) {
  _.each(methodMap, function (handler, name) {
    wrapMethodHanderForErrors(name, handler, methodMap);
  });
  return originalServerMethods.call(this, methodMap);
};
Meteor.methods = methodServerProto.methods.bind(Meteor.server);

// If REST loaded first, its single-method API has already captured the old registrar.
if (typeof Meteor.method === 'function') {
  var originalSingleMethod = Meteor.method;
  Meteor.method = function(name, handler, options) {
    var methodMap = Object.create(null);
    methodMap[name] = handler;
    wrapMethodHanderForErrors(name, handler, methodMap);
    return originalSingleMethod.call(this, name, methodMap[name], options);
  };
}


function wrapMethodHanderForErrors(name, originalHandler, methodMap) {
  if (typeof originalHandler !== 'function' || wrappedMethodHandlers.get(originalHandler) === name) return;

  methodMap[name] = trackMethodTiming(name, function () {
    try {
      return originalHandler.apply(this, arguments);
    } catch (ex) {
      if (ex && Kadira._getInfo()) {
        // sometimes error may be just an string or a primitive
        // in that case, we need to make it a psuedo error


        const user = Meteor.user();

        if (typeof ex !== 'object') {
          ex = { message: ex, stack: ex };
        }

        if (typeof ex.reason == 'object') {
          if (user) {
            ex.message = user.username + " ";
          }
          ex.message += name;

          if (ex.details) {
            ex.message += " " + ex.details;
          }

        }
        console.log("Kadira.wrapMethodHanderForErrors 1", EJSON.stringify(ex))


        // Now we are marking this error to get tracked via methods
        // But, this also triggers a Meteor.debug call and 
        // it only gets the stack
        // We also track Meteor.debug errors and want to stop 
        // tracking this error. That's why we do this
        // See Meteor.debug error tracking code for more

        const reason = ex.reason ? EJSON.stringify(ex.reason) : "";


        let stack = "";
        if (user)
          stack += "User: " + user.username + "\n";

        if (reason)
          stack += "Reason: " + reason + "\n";

        stack += ex.stack;



        ex.stack = { stack, source: 'method', };

        console.log("Kadira.wrapMethodHanderForErrors", ex)

        Kadira._getInfo().currentError = ex;
      }
      throw ex;
    }
  });
  wrappedMethodHandlers.set(methodMap[name], name);
}

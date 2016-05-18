// Copyright IBM Corp. 2016. All Rights Reserved.
// Node module: loopback-connector-db2z
// This file is licensed under the Artistic License 2.0.
// License text available at https://opensource.org/licenses/Artistic-2.0

/*!
 * DB2Z connector for LoopBack
 */
var IBMDB = require('loopback-connector-ibmdb').IBMDB;
var util = require('util');
var debug = require('debug')('loopback:connector:db2z');
var async = require('async');

/**
 * Initialize the IBMDB connector for the given data source
 *
 * @param {DataSource} ds The data source instance
 * @param {Function} [cb] The cb function
 */
exports.initialize = function(ds, cb) {
  ds.connector = new DB2Z(ds.settings);
  ds.connector.dataSource = ds;
  if (cb) {
    if (ds.settings.lazyConnect) {
      process.nextTick(function() {
        cb();
      });
    } else {
      ds.connector.connect(cb);
    }
  }
};

function DB2Z(settings) {
  debug('DB2Z constructor settings: %j', settings);
  IBMDB.call(this, 'db2z', settings);
}

util.inherits(DB2Z, IBMDB);

/**
 * Create the data model in DB2Z
 *
 * @param {string} model The model name
 * @param {Object} data The model instance data
 * @param {Object} options Options object
 * @param {Function} [callback] The callback function
 */
DB2Z.prototype.create = function(model, data, options, callback) {
  var self = this;
  var stmt = self.buildInsert(model, data, options);
  var id = self.idName(model);
  var sql = 'SELECT \"' + id + '\" FROM FINAL TABLE (' + stmt.sql + ')';
  self.execute(sql, stmt.params, options, function(err, info) {
    if (err) {
      callback(err);
    } else {
      callback(err, info[0][id]);
    }
  });
};

/**
 * Update all instances that match the where clause with the given data
 *
 * @param {string} model The model name
 * @param {Object} where The where object
 * @param {Object} data The property/value object representing changes
 * to be made
 * @param {Object} options The options object
 * @param {Function} cb The callback function
 */
DB2Z.prototype.update = function(model, where, data, options, cb) {
  var self = this;
  var stmt = self.buildUpdate(model, where, data, options);
  var id = self.idName(model);
  var sql = 'SELECT \"' + id + '\" FROM FINAL TABLE (' + stmt.sql + ')';
  self.execute(sql, stmt.params, options, function(err, info) {
    if (cb) {
      cb(err, {count: info.length});
    }
  });
};

/**
 * Delete all matching model instances
 *
 * @param {string} model The model name
 * @param {Object} where The where object
 * @param {Object} options The options object
 * @param {Function} cb The callback function
 */
DB2Z.prototype.destroyAll = function(model, where, options, cb) {
  var self = this;
  var stmt = self.buildDelete(model, where, options);
  var id = self.idName(model);
  var sql = 'SELECT \"' + id + '\" FROM OLD TABLE (' + stmt.sql + ')';
  self.execute(sql, stmt.params, options, function(err, info) {
    if (cb) {
      cb(err, {count: info.length});
    }
  });
};

require('./migration')(DB2Z);
require('./discovery')(DB2Z);
require('./transaction')(DB2Z);

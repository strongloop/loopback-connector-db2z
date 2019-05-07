// Copyright IBM Corp. 2016,2017. All Rights Reserved.
// Node module: loopback-connector-db2z
// This file is licensed under the Artistic License 2.0.
// License text available at https://opensource.org/licenses/Artistic-2.0

'use strict';

var g = require('./globalize');
var debug = require('debug')('loopback:connector:db2z:transaction');
var Transaction = require('loopback-connector').Transaction;

module.exports = mixinTransaction;

var mapIsolationLevel = function(isolationLevelString) {
  var ret = 2;
  switch (isolationLevelString) {
    case Transaction.READ_UNCOMMITTED:
      ret = 1;
      break;
    case Transaction.REPEATABLE_READ:
      ret = 4;
      break;
    case Transaction.SERIALIZABLE:
      ret = 8;
      break;
    case Transaction.READ_COMMITTED:
    default:
      ret = 2;
      break;
  }

  return ret;
};

/*!
 * @param {DB2Z} DB2Z connector class
 */
function mixinTransaction(DB2Z, db2z) {
  /**
   * Begin a new transaction

   * @param {Integer} isolationLevel
   * @param {Function} cb
   */
  DB2Z.prototype.beginTransaction = function(isolationLevel, cb) {
    debug('DB2Z.prototype.beginTransaction isolationLevel: %s', isolationLevel);

    var self = this;

    self.client.open(self.connStr, function(err, connection) {
      if (err) return cb(err);

      if (isolationLevel) {
        connection.setIsolationLevel(mapIsolationLevel(isolationLevel));
      }

      connection.beginTransaction(function(err) {
        cb(err, connection);
      });
    });
  };

  /**
   * Commit a transaction
   *
   * @param {Object} connection
   * @param {Function} cb
   */
  DB2Z.prototype.commit = function(connection, cb) {
    debug('Commit a transaction');
    connection.commitTransaction(function(err) {
      if (err) return cb(err);
      connection.close(cb);
    });
  };

  /**
   * Roll back a transaction
   *
   * @param {Object} connection
   * @param {Function} cb
   */
  DB2Z.prototype.rollback = function(connection, cb) {
    debug('Rollback a transaction');
    connection.rollbackTransaction(function(err) {
      if (err) return cb(err);
      // connection.setAutoCommit(true);
      connection.close(cb);
    });
  };
}

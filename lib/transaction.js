// Copyright IBM Corp. 2016. All Rights Reserved.
// Node module: loopback-connector-db2z
// This file is licensed under the Artistic License 2.0.
// License text available at https://opensource.org/licenses/Artistic-2.0

var debug = require('debug')('loopback:connector:db2z:transaction');
var Transaction = require('loopback-connector').Transaction;

module.exports = mixinTransaction;

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
    debug('Begin a transaction with isolation level: %s', isolationLevel);

    var self = this;

    if (isolationLevel !== Transaction.READ_COMMITTED &&
      isolationLevel !== Transaction.SERIALIZABLE) {
      var err = new Error('Invalid isolationLevel: ' + isolationLevel);
      err.statusCode = 400;
      return process.nextTick(function() {
        cb(err);
      });
    }

    self.connStr += ';IsolationLevel=ReadCommitted';

    self.client.open(self.connStr, function(err, connection) {
      if (err) return cb(err);
      connection.beginTransaction(function(err) {
        if (isolationLevel) {
          // This is currently not working so we are running with the
          // default.

          // var sql = 'CHANGE ISOLATION TO CS'; // + isolationLevel;

          // if (sql) {
          //   connection.query(sql, function(err) {
          //     cb(err, connection);
          //   });
          // } else {
          //   cb(err, connection);
          // }

          cb (err, connection);
        } else {
          cb(err, connection);
        }
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

// Copyright IBM Corp. 2016. All Rights Reserved.
// Node module: loopback-connector-db2z
// This file is licensed under the Artistic License 2.0.
// License text available at https://opensource.org/licenses/Artistic-2.0

'use strict';

/*!
 * DB2Z connector for LoopBack
 */
var IBMDB = require('loopback-ibmdb').IBMDB;
var util = require('util');
var debug = require('debug')('loopback:connector:db2z');
var ParameterizedSQL = require('loopback-connector').ParameterizedSQL;
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

  cb();
};

function DB2Z(settings) {
  debug('DB2Z constructor settings: %j', settings);
  IBMDB.call(this, 'db2z', settings);

  // This is less than ideal, better idea would be
  // to extend the propagation of the filter object
  // to executeSQL or pass the options obj around
  this.limitRE = /LIMIT (\d+)/;
  this.offsetRE = /OFFSET (\d+)/;
};

util.inherits(DB2Z, IBMDB);

DB2Z.prototype.setConnectionProperties = function(name, settings) {
  debug('DB2Z.prototype.setConnectionProperties');
  var self = this;
  self.dbname = (settings.database || settings.db || 'testdb');
  self.dsn = settings.dsn;
  self.hostname = (settings.hostname || settings.host);
  self.username = (settings.username || settings.user);
  self.password = settings.password;
  self.portnumber = settings.port;
  self.protocol = (settings.protocol || 'TCPIP');

  // Save off the connectionOptions passed in for connection pooling
  self.connectionOptions = {};
  self.connectionOptions.minPoolSize = parseInt(settings.minPoolSize, 10) || 0;
  self.connectionOptions.maxPoolSize = parseInt(settings.maxPoolSize, 10) || 0;
  self.connectionOptions.connectionTimeout =
    parseInt(settings.connectionTimeout, 10) || 60;

  var dsn = settings.dsn;
  if (dsn) {
    self.connStr = dsn;

    var DSNObject = self.parseDSN(dsn);
    self.schema = DSNObject.CurrentSchema || DSNObject.UID;
  } else {
    var connStrGenerate =
      'DRIVER={' + name + '}' +
      ';DATABASE=' + this.dbname +
      ';HOSTNAME=' + this.hostname +
      ';UID=' + this.username +
      ';PWD=' + this.password +
      ';PORT=' + this.portnumber +
      ';PROTOCOL=' + this.protocol;
    self.connStr = connStrGenerate;

    self.schema = this.username;
    if (settings.schema) {
      self.schema = settings.schema.toUpperCase();
    }
  }
};

DB2Z.prototype.createTable = function(model, cb) {
  debug('DB2Z.prototype.createTable');
  var self = this;
  var tableName = self.tableEscaped(model);
  var tableSchema = self.schema;
  var columnDefinitions = self.buildColumnDefinitions(model);
  var tasks = [];
  var options = {
    noResultSet: true,
  };

  tasks.push(function(callback) {
    var sql = 'CREATE TABLE ' + tableSchema + '.' + tableName +
              ' (' + columnDefinitions + ') CCSID UNICODE;';
    self.execute(sql, null, options, callback);
  });

  var indexes = self.buildIndexes(model);
  indexes.forEach(function(i) {
    tasks.push(function(callback) {
      self.execute(i, null, options, callback);
    });
  });

  async.series(tasks, cb);
};

/**
 * Create the data model in DB2Z
 *
 * @param {string} model The model name
 * @param {Object} data The model instance data
 * @param {Object} options Options object
 * @param {Function} [callback] The callback function
 */
DB2Z.prototype.create = function(model, data, options, callback) {
  debug('DB2Z.prototype.create: model=%s, data=%j, options=%j',
        model, data, options);
  var self = this;
  var stmt = self.buildInsert(model, data, options);
  var idName = self.idColumn(model);
  var sql;
  var statement = new ParameterizedSQL();

  if (data[idName]) {
    statement.merge(stmt);
  } else {
    statement.merge('SELECT ' + self.escapeName(idName) + ' FROM FINAL ' +
                    'TABLE ( ');
    statement.merge(stmt);
    statement.merge(')');
  }

  self.execute(statement.sql, statement.params, options, function(err, info) {
    if (err) {
      callback(err);
    } else {
      if (data[idName]) return callback(err, data[idName]);

      callback(err, info[0][idName]);
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
  debug('DB2Z.prototype.update: model=%s, where=%j, data=%j, options=%j',
        model, where, data, options);
  var self = this;
  var stmt = self.buildUpdate(model, where, data, options);
  var idName = self.idColumn(model);
  var sql = 'SELECT ' + self.escapeName(idName) + ' FROM FINAL ' +
            'TABLE (' + stmt.sql + ')';
  self.execute(sql, stmt.params, options, function(err, info) {
    if (cb) {
      cb(err, {count: info.length});
    }
  });
};

DB2Z.prototype.buildReplace = function(model, where, data, options) {
  process.nextTick(function() {
    throw Error(g.f('Function {{buildReplace}} not supported'));
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
  debug('DB2Z.prototype.destroyAll: model=%s, where=%j, options=%j',
        model, where, options);
  var self = this;
  var stmt = self.buildDelete(model, where, options);
  var idName = self.idColumn(model);
  var sql = 'SELECT ' + self.escapeName(idName) + ' FROM OLD ' +
            'TABLE (' + stmt.sql + ')';

  self.execute(sql, stmt.params, options, function(err, info) {
    if (cb) {
      cb(err, {count: info.length});
    }
  });
};

require('./migration')(DB2Z);
require('./discovery')(DB2Z);
require('./transaction')(DB2Z);

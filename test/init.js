// Copyright IBM Corp. 2016. All Rights Reserved.
// Node module: loopback-connector-db2z
// This file is licensed under the Artistic License 2.0.
// License text available at https://opensource.org/licenses/Artistic-2.0
'use strict';

var describe = require('./describe');

module.exports = require('should');

var DataSource = require('loopback-datasource-juggler').DataSource;

var config = {
  username: process.env.DB2Z_USERNAME,
  password: process.env.DB2Z_PASSWORD,
  hostname: process.env.DB2Z_HOSTNAME || 'localhost',
  port: process.env.DB2Z_PORTNUM || 60000,
  database: process.env.DB2Z_DATABASE || 'testdb',
  setMinPoolSize: 10,
  setMaxPoolSize: 500,
};

global.config = config;

global.getDataSource = global.getSchema = function(options) {
  var db = new DataSource(require('../'), config);
  return db;
};

global.connectorCapabilities = {
  ilike: false,
  nilike: false,
};

global.sinon = require('sinon');

// Copyright IBM Corp. 2016,2017. All Rights Reserved.
// Node module: loopback-connector-db2z
// This file is licensed under the Artistic License 2.0.
// License text available at https://opensource.org/licenses/Artistic-2.0

'use strict';

var describe = require('./describe');
require('./init.js');

/* eslint-env node, mocha */
describe('db2 imported features', function() {
  require('loopback-datasource-juggler/test/common.batch.js');
  require('loopback-datasource-juggler/test/include.test.js');
});

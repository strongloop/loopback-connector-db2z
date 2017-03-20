// Copyright IBM Corp. 2016. All Rights Reserved.
// Node module: loopback-connector-db2z
// This file is licensed under the Artistic License 2.0.
// License text available at https://opensource.org/licenses/Artistic-2.0

'use strict';

var g = require('./globalize');

/*!
 * DB2Z connector for LoopBack
 */
var async = require('async');
var debug = require('debug')('loopback:connector:db2z');

module.exports = function(DB2Z) {
  DB2Z.prototype.searchForPropertyInActual =
  function(model, propName, actualFields) {
    process.nextTick(function() {
      throw new Error(g.f('{{searchForPropertyInActual()}} is ' +
      'not currently supported.'));
    });
  };

  DB2Z.prototype.addPropertyToActual = function(model, propName) {
    process.nextTick(function() {
      throw new Error(g.f('{{addPropertyToActual()}} is ' +
      'not currently supported.'));
    });
  };

  DB2Z.prototype.columnDataType = function(model, property) {
    process.nextTick(function() {
      throw new Error(g.f('{{columnDataType()}} is not currently supported.'));
    });
  };

  DB2Z.prototype.buildColumnType = function(property) {
    process.nextTick(function() {
      throw new Error(g.f('{{buildColumnType()}} is not currently supported.'));
    });
  };

  DB2Z.prototype.propertyHasNotBeenDeleted = function(model, propName) {
    process.nextTick(function() {
      throw new Error(g.f('{{propertyHasNotBeenDeleted()}} is ' +
      'not currently supported.'));
    });
  };

  DB2Z.prototype.applySqlChanges =
  function(model, pendingChanges, cb) {
    process.nextTick(function() {
      return cb(Error(g.f('{{applySqlChanges()}} is not ' +
      'currently supported.')));
    });
  };

  DB2Z.prototype.showFields = function(model, cb) {
    process.nextTick(function() {
      return cb(Error(g.f('{{showFields()}} is not currently supported.')));
    });
  };

  DB2Z.prototype.showIndexes = function(model, cb) {
    process.nextTick(function() {
      return cb(Error(g.f('{{showIndexes()}} is not currently supported.')));
    });
  };

  /*
   * Perform autoupdate for the given models
   * @param {String[]} [models] A model name or an array of model names.
   * If not present, apply to all models
   * @param {Function} [cb] The callback function
   */
  DB2Z.prototype.autoupdate = function(models, cb) {
    debug('DB2Z.prototype.autoupdate %j', models);
    var self = this;

    if ((!cb) && (typeof models === 'function')) {
      cb = models;
      models = undefined;
    }
    // First argument is a model name
    if (typeof models === 'string') {
      models = [models];
    }

    models = models || Object.keys(this._models);

    async.each(models, function(model, done) {
      if (!(model in self._models)) {
        return process.nextTick(function() {
          done(new Error(g.f('Model not found: %s', model)));
        });
      }
      self.getTableStatus(model, function(err, fields, indexes) {
        if (err) {
          return done(err);
        } else {
          // For DB2Z on z/OS we need to destroy the model and
          // recreate because we can't get all the data required
          // to determine if the model is valid.
          self.dropTable(model, function(err, cnt) {
            if (err) {
              return done(Error(err));
            }

            self.createTable(model, done);
          });
        }
      });
    }, cb);
  };

  /*
   * Discover the properties from a table
   * @param {String} model The model name
   * @param {Function} cb The callback function
   */
  DB2Z.prototype.getTableStatus = function(model, cb) {
    var self = this;
    var columnSQL, indexSQL;

    columnSQL = 'SELECT NAME, COLTYPE AS DATATYPE, COLNO, ' +
      'LENGTH AS DATALENGTH, NULLS FROM SYSIBM.SYSCOLUMNS WHERE ' +
      'TBNAME LIKE \'' + self.table(model) + '\' ' +
      'AND TBCREATOR LIKE \'' + self.schema + '\' ' +
      'ORDER BY COLNO';

    self.execute(columnSQL, function(err, tableInfo) {
      if (err) {
        cb(err);
      } else {
        indexSQL = 'SELECT TBNAME AS TABNAME, CREATOR AS TABSCHEMA, ' +
          'NAME AS INDNAME, UNIQUERULE FROM SYSIBM.SYSINDEXES ' +
          'WHERE TBNAME LIKE \'' + self.table(model) + '\' ' +
          'AND CREATOR LIKE \'' + self.schema + '\'';

        self.execute(indexSQL, function(err, indexInfo) {
          if (err) {
            console.log(err);
          }
          cb(err, tableInfo, indexInfo);
        });
      }
    });
  };

  DB2Z.prototype.getColumnsToAdd = function(model, actualFields) {
    var self = this;
    var m = this.getModelDefinition(model);
    var propNames = Object.keys(m.properties).filter(function(name) {
      return !!m.properties[name];
    });
    var operations = [];

    // change/add new fields
    propNames.forEach(function(propName) {
      if (m.properties[propName] && self.id(model, propName)) return;
      var found;
      if (actualFields) {
        actualFields.forEach(function(f) {
          if (f.NAME === propName) {
            found = f;
          }
        });
      }

      if (found) {
        actualize(propName, found);
      } else {
        operations.push('ADD COLUMN ' + propName + ' ' +
          self.buildColumnDefinition(model, propName));
      }
    });

    function actualize(propName, oldSettings) {
      var newSettings = m.properties[propName];
      if (newSettings && changed(newSettings, oldSettings)) {
        // TODO: NO TESTS EXECUTE THIS CODE PATH
        var pName = '\'' + propName + '\'';
        operations.push('CHANGE COLUMN ' + pName + ' ' + pName + ' ' +
          self.buildColumnDefinition(model, propName));
      }
    }

    function changed(newSettings, oldSettings) {
      if (oldSettings.Null === 'YES') {
        // Used to allow null and does not now.
        if (!self.isNullable(newSettings)) {
          return true;
        }
      }
      if (oldSettings.Null === 'NO') {
        // Did not allow null and now does.
        if (self.isNullable(newSettings)) {
          return true;
        }
      }

      return false;
    }
    return operations;
  };

  DB2Z.prototype.getColumnsToDrop = function(model, actualFields) {
    var self = this;
    var m = this._models[model];
    var propNames = Object.keys(m.properties).filter(function(name) {
      return !!m.properties[name];
    });
    var operations = [];

    // drop columns
    if (actualFields) {
      actualFields.forEach(function(f) {
        var notFound = !~propNames.indexOf(f.NAME);
        if (m.properties[f.NAME] && self.id(model, f.NAME)) return;
        if (notFound || !m.properties[f.NAME]) {
          operations.push('DROP COLUMN ' + f.NAME);
        }
      });
    }
    return operations;
  };

  DB2Z.prototype.addIndexes = function(model, actualIndexes) {
    var self = this;
    var m = this.getModelDefinition(model);
    var propNames = Object.keys(m.properties).filter(function(name) {
      return !!m.properties[name];
    });
    var indexes = m.settings.indexes || {};
    var indexNames = Object.keys(indexes).filter(function(name) {
      return !!m.settings.indexes[name];
    });
    var sql = [];
    var tasks = [];
    var operations = [];
    var ai = {};
    var type = '';

    if (actualIndexes) {
      actualIndexes.forEach(function(i) {
        var name = i.INDNAME;
        if (!ai[name]) {
          ai[name] = {
            info: i,
            columns: [],
          };
        }

        i.COLNAMES.split(/\+\s*/).forEach(function(columnName, j) {
          // This is a bit of a dirty way to get around this but DB2Z returns
          // column names as a string started with and separated by a '+'.
          // The code below will strip out the initial '+' then store the
          // actual column names.
          if (j > 0)
            ai[name].columns[j - 1] = columnName;
        });
      });
    }
    var aiNames = Object.keys(ai);

    // remove indexes
    aiNames.forEach(function(indexName) {
      if (ai[indexName].info.UNIQUERULE === 'P' || // indexName === 'PRIMARY' ||
        (m.properties[indexName] && self.id(model, indexName))) return;

      if (indexNames.indexOf(indexName) === -1 && !m.properties[indexName] ||
        m.properties[indexName] && !m.properties[indexName].index) {
        if (ai[indexName].info.UNIQUERULE === 'P') {
          operations.push('DROP PRIMARY KEY');
        } else if (ai[indexName].info.UNIQUERULE === 'U') {
          operations.push('DROP UNIQUE ' + indexName);
        }
      } else {
        // first: check single (only type and kind)
        if (m.properties[indexName] && !m.properties[indexName].index) {
          // TODO
          return;
        }
        // second: check multiple indexes
        var orderMatched = true;
        if (indexNames.indexOf(indexName) !== -1) {
          m.settings.indexes[indexName].columns.split(/,\s*/).forEach(
            function(columnName, i) {
              if (ai[indexName].columns[i] !== columnName) orderMatched = false;
            });
        }

        if (!orderMatched) {
          if (ai[indexName].info.UNIQUERULE === 'P') {
            operations.push('DROP PRIMARY KEY');
          } else if (ai[indexName].info.UNIQUERULE === 'U') {
            operations.push('DROP UNIQUE ' + indexName);
          }

          delete ai[indexName];
        }
      }
    });

    if (operations.length) {
      // Add the ALTER TABLE statement to the list of tasks to perform later.
      sql.push('ALTER TABLE ' + self.schema + '.' +
               self.tableEscaped(model) + ' ' + operations.join(' ') + ';');
    }

    // add single-column indexes
    propNames.forEach(function(propName) {
      var i = m.properties[propName].index;
      if (!i) {
        return;
      }
      var found = ai[propName] && ai[propName].info;
      if (!found) {
        var pName = propName;
        type = '';
        if (i.type) {
          type = i.type;
        }
        sql.push('CREATE ' + type + ' INDEX ' + pName + ' ON ' +
                 self.schema + '.' + self.tableEscaped(model) +
                 '(\"' + pName + '\") ');
      }
    });

    // add multi-column indexes
    indexNames.forEach(function(indexName) {
      var i = m.settings.indexes[indexName];
      var found = ai[indexName] && ai[indexName].info;
      if (!found) {
        var iName = indexName;
        var type = '';
        if (i.type) {
          type = i.type;
        }
        var stmt = 'CREATE ' + type + 'INDEX ' + iName + ' ON ' +
                   self.schema + '.' + self.tableEscaped(model) + '(';

        var splitNames = i.columns.split(/,\s*/);
        var colNames = splitNames.join('\",\"');

        stmt += '\"' + colNames + '\")';

        sql.push(stmt);
      }
    });
    return sql;
  };

  DB2Z.prototype.isActual = function(models, cb) {
    debug('DB2Z.prototype.isActual %j %j', models, cb);
    var self = this;

    if ((!cb) && (typeof models === 'function')) {
      cb = models;
      models = undefined;
    }

    // First argument is a model name
    if (typeof models === 'string') {
      models = [models];
    }

    models = models || Object.keys(this._models);

    // var changes = [];
    async.each(models, function(model, done) {
      self.getTableStatus(model, function(err, fields, indexes) {
        if (err) {
          return done(Error(err));
        }
        // TODO: VALIDATE fields/indexes against model definition
        done();
      });
    }, function done(err) {
      if (err) {
        return cb && cb(err);
      }
      var actual = true; // (changes.length === 0);
      if (cb) cb(null, actual);
    });
  };
};

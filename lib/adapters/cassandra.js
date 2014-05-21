var Util = require('util');
// var Helenus = require('helenus');
var Cql = require('node-cassandra-cql');

var AbstractModel = require('../abstract');

var defaultQueryOptions = Cql.types.consistencies.one;

// var defaultQueryOptions = { consistencyLevel: Helenus.ConsistencyLevel.ONE };

var CassandraAdapter = function (options) {
    return AbstractModel.call(this, options);
};
Util.inherits(CassandraAdapter, AbstractModel);


CassandraAdapter.connect = function (options, callback) {
    // CassandraAdapter.connection = new Helenus.ConnectionPool(options);
    
    CassandraAdapter.connection = new Cql.Client(options);
    
    // CassandraAdapter.connection.on('error', function(err){
    //     console.error(err.name, err.message);
    // });
    // CassandraAdapter.connection.connect(function(err, keyspace){
    //     if (err){
    //         console.error(err);
    //     } else {
    //         console.log('Connected to Cassandra Pool:', options.hosts);
    //     }
    //     callback(err, CassandraAdapter.connection);
    // });

    callback(null, CassandraAdapter.connection);
};


CassandraAdapter.prototype.writeClause = function (obj, callback) {
    var columns = Object.keys(obj);
    var valuesHolder = columns.map(function(d){ return "?";});
    var values = columns.map(function(d){ return obj[d];}); // TODO: move into one loop
    
    return callback(null, "INSERT INTO " + this.options.tableName + " (" + columns.join(", ") + ") VALUES (" + valuesHolder.join(", ") + ")", values);
};


CassandraAdapter.prototype.write = function (obj, options, callback) {
    var self = this;
    if (typeof options == 'function') {
        callback = options;
        options = defaultQueryOptions;
    }
    if (options == null) {
        options = defaultQueryOptions;
    }
    this.writeClause(obj, function(err, clause, values) {
        if (err) {
            return callback(err, clause, values);
        }
        // console.log(clause, values, options, typeof callback)
        CassandraAdapter.connection.execute(clause, values, options, callback);
    });
};


CassandraAdapter.prototype.exec = function (query, predicates, options, callback) {
    CassandraAdapter.connection.execute(query, predicates, options, callback);
};


CassandraAdapter.prototype.readClause = function (terms, callback) {
    var clause = "SELECT * FROM " + this.options.tableName;
    if (terms !== null) {
        if (typeof terms == 'string') {
            terms = [terms];
        }
        var termsHolder = terms.map(function(d){ return "?"; });
         clause+= " WHERE " + this.options.primaryKey + " IN (" + termsHolder + ")";
    } else {
        terms = [];
    }
    return callback(null, clause, terms);
};


CassandraAdapter.prototype.read = function (terms, options, callback) {
    if (typeof options == 'function') {
        callback = options;
        options = defaultQueryOptions;
    }
    if (options == null) {
        options = defaultQueryOptions;
    }
    var self = this;
    
    this.readClause(terms, function (err, clause, predicates) {
        if (err) {
            return callback(err, clause, predicates);
        }
        // console.log(clause, predicates);
        CassandraAdapter.connection.execute(clause, predicates, options, function (err, result) {
            var output = [];
            
            result.rows.forEach(function(el){
                
                var InheritedModel = function (options) {
                    return CassandraAdapter.call(this, options);
                };
                Util.inherits(InheritedModel, CassandraAdapter);
                var Abstract = new InheritedModel(self.options);
                var abs = new Abstract();
                
                Object.keys(el).forEach(function(key){
                    if (key == 'columns') {
                        return;
                    }
                    
                    if (el[key]) {
                        abs[key] = el[key];
                    }
                })
                output.push(abs);
            });
            
            callback(err, output);
        });
    });
};


CassandraAdapter.prototype.close = function () {
    CassandraAdapter.connection && CassandraAdapter.connection.close();
};

module.exports = CassandraAdapter;
var CassandraAdapter = require('./adapters/cassandra');

var Horm = function () {
    this.connection = null;
};

Horm.prototype.connect = function (adapter, options, callback) {
    if (adapter == 'cassandra') {
        console.log(options)
        var self = this;
        CassandraAdapter.connect(options, function(err, conn){
            if (err) {
                throw err;
            }
            self.connection = conn;
            callback(err);
        });
    } else {
        throw "Unsupported adapter: " + adapter;
    }
};

module.exports = new Horm();
module.exports.AbstractModel = require('./abstract');
module.exports.Adapters = {};
module.exports.Adapters.Cassandra = CassandraAdapter;
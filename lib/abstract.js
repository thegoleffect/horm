
var defaultOptions = {
    tableName: '',
    disallowedKeys: [
        'set', 'get', 'save'
    ]
};


var AbstractModel = function (options) {
    this.options = options || {};
    console.log(this.options)
    
    var self = this;
    var Abstract = function (values) {
        var innerSelf = this;
        if (typeof values == 'object') {
            Object.keys(values).forEach(function(d){
                innerSelf.set(d, values[d]);
            });
        }
    };
    
    Abstract.prototype.set =
    Abstract.prototype.get = function (key, value) {
        if (self.validateKey(key)) {
            throw "The key (" + key + ") is not allowed.";
        }
        
        if (typeof value !== 'undefined') {
            this[key] = value;
        }
        
        return this[key];
    };
    
    Abstract.prototype.save = function (options, callback) {
        if (typeof options == 'function') {
            callback = options;
            options = {};
        }

        self.preSave(this, function(err, obj) {
            console.log(obj);
            
            self.insert(obj, options, function (err, results) {
                if (err) {
                    return callback(err, results);
                }
                
                self.postSave(callback);
            });
        });
    };
    
    Abstract.prototype.find = function (query, callback)  {
        self.findAll(query, callback);
    };
    
    Abstract.prototype.exec = function (query, predicates, options, callback) {
        self.exec(query, predicates, options, callback);
    };
    
    return Abstract;
};


AbstractModel.prototype.validateKey = function (key) {
    if (this.options.disallowedKeys.indexOf(key) >= 0) {
        return false;
    }
    return true;
};


AbstractModel.prototype.preSave = function(ref, callback) {
    // console.log(JSON.stringify(ref))
    callback(null, ref);
};


AbstractModel.prototype.postSave = function (callback) {
    return callback(null);
};


AbstractModel.prototype.findAll = function (terms, options, callback) {
    this.read(terms, options, callback);
};


// AbstractModel.prototype.writeClause = function (obj, callback) {
//     var clause = "INSERT INTO";
//     var predicate = [];
//     return callback(null, clause, predicate);
// };


AbstractModel.prototype.insert = function (obj, options, callback) {
    this.write(obj, options, callback);
};


AbstractModel.prototype.close = function () {
    this.connection && this.connection.close();
};


module.exports = AbstractModel;
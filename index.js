/**
 * Created by Shyam on 1/23/2018.
 */

const
    builder = require('mongo-qbuilder'),
    utils = require('utils-data'),
    express = require('express'),
    ObjectId = require('mongodb').ObjectId,

    // for parsing multipart/form-data, along with files, stored in memory
    multer = require('multer'),
    formDataAndInMemoryFiles = multer({storage: multer.memoryStorage()});

/**
 * Enum of HTTP status codes used.
 * @enum {number}
 */
const HTTP_STATUS = {
    OK: 200,
    BAD_REQUEST: 400,
    INTERNAL_SERVER_ERROR: 500
};

/**
 * Enum of possible Data Types of {@link FieldFormat}.type.
 * @enum {string}
 */
const DataType = {
    string: "string",
    number: "number",
    objectId: "objectId"
};

/**
 * @typedef {{}} APIConfiguration
 * @property {boolean|Object<string, Array<{}>>} [read=false] If failse, the API will not have a GET method to retrieve data. Filtering is possible using URL params.
 * It can optionally be a map of aggregation pipelines to be used on the filtered result. Apart from the main GET API <collection path>/, GET API endpoints are created
 * for each aggregation as <collection path>/<aggregation key>.
 * @property {boolean} [create=false] Whether the API should have a POST method to insert.
 * @property {boolean} [update=false] If mentioned, the API's POST & PUT methods will allow data update, wherever the _id key is mentioned.
 * POST form data should have a 'data' with the array of objects to be inserted or updated.
 * @property {boolean} [delete=false] If mentioned, the API's DELETE method will allow data deletion for all data matched with the URL params.
 * @property {Object<string,FieldFormat>} fields Map of db field to parameter formats.
 */
/**
 * @typedef {{}} FieldFormat
 * @property {DataType|function(value:*,input:Object,isFilter:boolean):*} type Type of the field, which can either be one of the hard-coded
 * types mentioned in DataType, or a function that returns the value, by taking the parsed value, the whole input map, and the API method, viz.
 * "read", "create", "update" or "delete".
 * @property {string} param Request parameter.
 * @property {boolean} [read=true] Whether this field should be used while generating the filter to retrieve data.
 * @property {boolean} [delete=true] Whether this field should be used while generating the filter to delete data.
 * @property {boolean} [write=true] Whether this field can be inserted or changed or not (though the restriction may apply only to REST APIs).
 * @property {*} [default] Default value to be used if parameter is absent in the request.
 */

/**
 * @param {Router} router Express (package 'express) Router instance.
 * @param {Db} db Mongo (package 'mongodb') DB as obtained
 * @param {string} collection
 * @param {APIConfiguration} configuration
 * @private
 */
function _registerAPI(router, db, collection, configuration) {
    _validateConfiguration(collection, configuration);

    var path = "/" + collection;
    if (configuration.read) {
        var hasOverriddenPath = false;
        if (Object.getOwnPropertyNames(configuration.read).length) {
            var aggregationPath;
            for (var aggregationKey in configuration.read) {
                if (!configuration.read.hasOwnProperty(aggregationKey)) continue;
                aggregationPath = path;
                if (aggregationKey.length) aggregationPath += "/" + aggregationKey;
                else hasOverriddenPath = true;
                router.get(aggregationPath, _read(db, collection, configuration.fields, configuration.read[aggregationKey]));
            }
        }
        if (!hasOverriddenPath) router.get(path, _read(db, collection, configuration.fields));
    }
    if (configuration.delete) router.delete(path, _delete(db, collection, configuration.fields));
    if (configuration.create || configuration.update) {
        router.post(path, formDataAndInMemoryFiles.any(), _save(db, collection, configuration.fields, true, configuration.create, configuration.update));
        router.put(path, formDataAndInMemoryFiles.any(), _save(db, collection, configuration.fields, false, configuration.create, configuration.update));
    }
}

/**
 * @param {string} collection
 * @param {APIConfiguration} configuration
 * @private
 */
function _validateConfiguration(collection, configuration) {
    if (!configuration.read && !configuration.create && !configuration.update && !configuration.delete) throw "Invalid configuration for \"" + collection + "\" - is neither read, nor write, nor update, nor delete!";

    if (!configuration.fields) {
        // fields are not required for read only APIs
        if (!configuration.create && !configuration.update && !configuration.delete) return;
        else throw "Invalid configuration for \"" + collection + "\" - fields required for write, update or delete APIs!";
    }

    if (configuration.update && !configuration.fields._id) throw "Invalid configuration for \"" + collection + "\" - _id field required for update APIs!";

    var fields = Object.getOwnPropertyNames(configuration.fields);
    if (!fields.length) return;
    /**
     * @type {Object<string,{field:string, format:FieldFormat}>}
     */
    var byParams = {};
    for (var i = 0, field, propFormat, param; i < fields.length; ++i) {
        field = fields[i];
        propFormat = configuration.fields[field];
        param = propFormat.param || field;

        if (byParams[param]) throw "Invalid format for \"" + collection + "\" field \"" + field + "\"! Param name '" + param + "' is already in use for " + JSON.stringify(byParams[param].field) + ": " + JSON.stringify(byParams[param].format);
        else byParams[param] = {field: field, format: propFormat};

        if (!DataType.hasOwnProperty(propFormat.type) && !utils.isFunction(propFormat.type))
            throw "Unknown type " + JSON.stringify(propFormat.type) + " in format for \"" + collection + "\" field \"" + field + "\"!";
    }
    fields = null;
    byParams = null;
}

/**
 * @param req
 * @param res
 * @param {Object<string,FieldFormat>} [filterFormats]
 * @param {string} methodType "read", "create", "update", "delete"
 * @returns {*}
 * @private
 */
function _filterFromURLParams(req, res, filterFormats, methodType) {
    var filter;
    if (filterFormats && Object.getOwnPropertyNames(filterFormats).length) {
        // has filter.
        var qb = new builder.QueryBuilder();
        var fields = Object.getOwnPropertyNames(filterFormats);
        for (var i = 0, field, param, propFormat, value, values; i < fields.length; ++i) {
            field = fields[i];
            propFormat = filterFormats[field];

            if (propFormat[methodType] == false) {
                console.warn("Ignoring " + methodType + " request for restricted field " + field + ": " + JSON.stringify(propFormat));
                continue;
            }

            param = propFormat.param || field;
            values = req.query[param];
            if (values == null && methodType == "read") values = propFormat.default;
            if (values == null) continue;
            if (!Array.isArray(values)) values = [values];

            var formattedValues = [];
            for (var j = 0; j < values.length; ++j) {
                value = values[j];
                try {
                    if (utils.isFunction(propFormat.read)) {
                        value = propFormat.read(value, req.query, param, methodType);
                    }
                    else if (propFormat.type == DataType.string) {
                        if (utils.isStr(value)) value = value.trim();
                        else throw "Absent or not a valid string";
                    }
                    else if (propFormat.type == DataType.objectId) {
                        if (utils.isValidStr(value)) value = new ObjectId(value.trim());
                        else if (!(value instanceof ObjectId)) throw "Absent or not a valid ObjectId";
                    }
                    else if (propFormat.type == DataType.number) {
                        /**
                         * A string "[from,to]" where both from and to are numeric but optional.
                         */
                        var REGEX_FOR_ARRAY = /\s*\[\s*([\d]*)\s*,\s*([\d]*)\s*]\s*/;

                        /**
                         * Both from and to are numeric but optional.
                         * @param {number} [from]
                         * @param {number} [to]
                         * @returns {{}}
                         * @private
                         */
                        function _numericRangeToQuery(from, to) {
                            var q = {};
                            if (utils.isNumeric(from)) q.$gte = from;
                            if (utils.isNumeric(to)) q.$lte = to;
                            return q;
                        }

                        var match;
                        if (utils.isNumeric(value)) {
                            value = parseFloat(value);
                        }
                        else if (Array.isArray(value) && value.length == 2
                            && (utils.isNumeric(value[0]) || utils.isNumeric(value[1]))) {
                            value = _numericRangeToQuery(parseFloat(value[0]), parseFloat(value[1]));
                        }
                        else if (utils.isValidStr(value) && (match = value.match(REGEX_FOR_ARRAY))) {
                            value = _numericRangeToQuery(parseFloat(match[1]), parseFloat(match[2]));
                        }
                        else throw "Neither a number nor a numeric range [from,to]"
                    }
                    else if (utils.isFunction(propFormat.type)) {
                        value = propFormat.type(value, req.query, param);
                    }
                    else {
                        console.error("Invalid type in formats: " + JSON.stringify(propFormat));
                        return res.status(HTTP_STATUS.INTERNAL_SERVER_ERROR).send("Invalid type in formats");
                    }

                    formattedValues.push(value);
                } catch (e) {
                    // skip invalid filter
                    console.warn("Invalid value [" + value + "] for formats: " + JSON.stringify(propFormat));
                }
            }
            if (formattedValues.length == 1) {
                qb.field(field).matches(formattedValues[0]);
            }
            else if (formattedValues.length) {
                qb.field(field).matchesAny(formattedValues);
            }
        }
        filter = qb.build();
    }
    return filter;
}

/**
 * Simple utility method to get data from a mongo collection.
 * @param {Db} db Mongo (package 'mongodb') DB as obtained
 * @param {string} collection
 * @param {Object<string,FieldFormat>} [filterFormats]
 * @param {Array.<{}>} [aggregation] Optional aggregation pipeline
 * to be performed on the filtered documents.
 */
function _read(db, collection, filterFormats, aggregation) {
    return function (req, res) {
        var filter = _filterFromURLParams(req, res, filterFormats, "read");

        // console.log("GET", collection, utils.JSONstringify(filter));
        var onFetched = function (err, results) {
            if (err) {
                console.error(err);
                return res.status(HTTP_STATUS.INTERNAL_SERVER_ERROR).send(err);
            }
            res.status(HTTP_STATUS.OK).send(results);
        };

        // todo: add project for field-to-param conversions
        if (aggregation && Array.isArray(aggregation) && aggregation.length) {
            var pipeline = [];
            if (filter && Object.getOwnPropertyNames(filter).length) pipeline.push({$match: filter});
            pipeline.push.apply(pipeline, aggregation);
            // console.log("Aggregation", collection, utils.JSONstringify(pipeline));
            db.collection(collection).aggregate(pipeline).toArray(onFetched);
        }
        else db.collection(collection).find(filter).toArray(onFetched);
    };
}

/**
 * Simple utility method to delete data from a mongo collection.
 * @param {Db} db Mongo (package 'mongodb') DB as obtained
 * @param {string} collection
 * @param {Object<string,FieldFormat>} [filterFormats]
 */
function _delete(db, collection, filterFormats) {
    return function (req, res) {
        var filter = _filterFromURLParams(req, res, filterFormats, "delete");

        console.log("DELETE", collection, utils.JSONstringify(filter));
        db.collection(collection).deleteMany(filter, null, function (err, results) {
            if (err) {
                console.error(err);
                return res.status(HTTP_STATUS.INTERNAL_SERVER_ERROR).send(err);
            }
            console.log(results);
            res.status(HTTP_STATUS.OK).send(results.result);
        });
    };
}

/**
 * Simple utility method to save a data from req.body parameters into a mongo collection.
 * @param {Db} db Mongo (package 'mongodb') DB as obtained
 * @param {string} collection
 * @param {Object<string,FieldFormat>} formats
 * @param {boolean} [multiple=false]
 * @param {boolean} [create=false]
 * @param {boolean} [update=false]
 */
function _save(db, collection, formats, multiple, create, update) {
    if (!create && !update) throw "Invalid write API registration - neither create nor update is allowed!";
    return function (req, res) {
        var insertDocuments = [];
        var updateDocuments = [];
        var inputs;
        if (multiple) {
            try {
                if (!Array.isArray(req.body.data)) req.body.data = JSON.parse(req.body.data);
                inputs = req.body.data;
            } catch (e) {
                return res.status(HTTP_STATUS.BAD_REQUEST).send("Missing or invalid 'data'! Should be an array.");
            }
            if (!Array.isArray(inputs)) return res.status(HTTP_STATUS.BAD_REQUEST).send("Invalid 'data'! Should be an array.");
            if (!inputs.length) return res.status(HTTP_STATUS.BAD_REQUEST).send("Empty 'data'!");
        }
        else {
            inputs = [req.body];
        }
        // put files
        if (req.files && req.files.length) for (var i = 0, file, match, field, data; i < req.files.length; ++i) {
            file = req.files[i];
            match = file.fieldname.match(/[^\[\]]+/g);
            if (!match || !match.length) continue;
            data = req.body;
            for (var j = 0; j < match.length; ++j) {
                field = match[j];
                if (j == match.length - 1) data[field] = file;
                else data = data[field];
            }
        }

        var fields = Object.getOwnPropertyNames(formats);
        for (var i = 0, input, data, isUpdating; i < inputs.length; ++i) {
            input = inputs[i];
            isUpdating = input._id != null;
            if (!update && isUpdating) {
                return res.status(HTTP_STATUS.BAD_REQUEST).send("Update not allowed! _id found: " + input._id);
            }

            data = {};
            for (var j = 0, field, param, propFormat, value; j < fields.length; ++j) {
                field = fields[j];
                propFormat = formats[field];

                if (propFormat.write == false) {
                    return res.status(HTTP_STATUS.BAD_REQUEST).send("Field '" + field + "' is not writable!");
                }

                param = propFormat.param || field;
                value = input[param] || propFormat.default;
                try {
                    if (utils.isFunction(propFormat.write)) {
                        value = propFormat.write(value, input, param, update && isUpdating ? "update" : "create");
                    }
                    else if (propFormat.type == DataType.string) {
                        if (utils.isStr(value)) value = value.trim();
                        else throw "Absent or not a valid string";
                        if (!value.length && !propFormat.hasOwnProperty("default")) throw "Can not be an empty string";
                    }
                    else if (propFormat.type == DataType.objectId) {
                        if (utils.isValidStr(value)) value = new ObjectId(value.trim());
                        else if (!(value instanceof ObjectId)) throw "Absent or not a valid ObjectId";
                    }
                    else if (propFormat.type == DataType.number) {
                        if (utils.isNumeric(value)) value = parseFloat(value);
                        else throw "Absent or not a valid numeric value";
                    }
                    else if (utils.isFunction(propFormat.type)) {
                        value = propFormat.type(value, input, param, update && isUpdating ? "update" : "create");
                    }
                    else {
                        // should never happen as the configuration is already validated at the beginning
                        console.error("Invalid type in format: " + JSON.stringify(propFormat));
                        return res.status(HTTP_STATUS.INTERNAL_SERVER_ERROR).send("Invalid type in format");
                    }
                } catch (e) {
                    if (isUpdating) continue; // skip only if updating
                    return res.status(HTTP_STATUS.BAD_REQUEST).send("Invalid '" + param + "': " + (e.message || e));
                }
                data[field] = value;
            }
            if (data._id != null && Object.getOwnPropertyNames(data).length) updateDocuments.push(data);
            else if (create) insertDocuments.push(data);
        }

        var responseResult = {inserted: []};
        if (update) {
            responseResult.updated = [];
            responseResult.upserted = [];
            responseResult.unchanged = [];
        }

        /**
         * @param err Error from preceding insertMany() operation
         * @param result Result from preceding insertMany() operation
         * @return {*}
         * @private
         */
        function _update(err, result) {
            if (err) {
                console.error(err);
                return res.status(HTTP_STATUS.INTERNAL_SERVER_ERROR).send(err);
            }
            if (result) {
                console.log("Inserted", result);
                if (result.insertedCount) {
                    var document;
                    for (var i = 0; i < result.insertedCount; ++i) {
                        document = insertDocuments[i];
                        document._id = result.insertedIds[i].toString();
                        responseResult.inserted.push(document);
                    }
                }
            }

            console.log("POST/PUT updating", collection, utils.JSONstringify(updateDocuments));
            if (!updateDocuments.length) return res.status(HTTP_STATUS.OK).send(responseResult);

            var waiting = updateDocuments.length;
            var lastErr;
            for (var j = 0, data; j < updateDocuments.length; ++j) {
                data = updateDocuments[j];
                // {$set : {"text" : "blah"}
                db.collection(collection).updateOne({"_id": data._id}, {"$set": data}, {"upsert": true}, function (data) {
                    return function (err, result) {
                        var document;
                        if (err) lastErr = err;
                        else if (result.upsertedCount) {
                            document = data;
                            document._id = result.upsertedId._id.toString();
                            responseResult.upserted.push(document);
                        }
                        else if (result.modifiedCount) {
                            document = data;
                            document._id = data._id.toString();
                            responseResult.updated.push(document);
                        }
                        else if (result.matchedCount) { // matchedCount may be 1 and modifiedCount 0 if no change was detected
                            document = data;
                            document._id = data._id.toString();
                            responseResult.unchanged.push(document);
                        }
                        if (--waiting) return;
                        console.log(err, result);

                        if (lastErr) {
                            console.error(err);
                            return res.status(HTTP_STATUS.INTERNAL_SERVER_ERROR).send(err);
                        }
                        else res.status(HTTP_STATUS.OK).send(responseResult);
                    };
                }(data));
            }
        }

        if (insertDocuments.length) {
            console.log("POST/PUT insert", collection, utils.JSONstringify(insertDocuments));
            db.collection(collection).insertMany(insertDocuments, null, _update);
        }
        else _update();
    }
}

/**
 * Entry point to create the REST APIs.
 *
 * @param {Db} db Mongo (package 'mongodb') DB as obtained
 * @param {Object<string,APIConfiguration>} configuration Collection to configuration mapping.
 */
function createEndpoints(db, configuration) {
    var collections = Object.getOwnPropertyNames(configuration);
    if (!collections.length) return;

    var router = express.Router();
    for (var i = 0, collection; i < collections.length; ++i) {
        collection = collections[i];
        _registerAPI(router, db, collection, configuration[collection]);
    }
    return router;
}

module.exports = {
    DataType: DataType,
    createEndpoints: createEndpoints
};
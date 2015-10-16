/*
 * pgjxfields.js: extract fields from a JSON-ified postgresql table dump.
 */

var mod_cmdutil = require('cmdutil');
var mod_jsprim = require('jsprim');
var mod_lstream = require('lstream');
var mod_stream = require('stream');
var mod_util = require('util');
var mod_vstream = require('vstream');
var JsonParserStream = require('vstream-json-parser');
var VError = require('verror');

function main()
{
	var lstream, parsestream, extractorstream;

	lstream = new mod_lstream();
	parsestream = new JsonParserStream();
	parsestream.on('warn', streamWarn);

	extractorstream = new ExtractorStream({}, process.argv.slice(2));
	extractorstream.on('warn', streamWarn);
	extractorstream.on('error', mod_cmdutil.fail);

	process.stdin.pipe(lstream);
	lstream.pipe(parsestream);
	parsestream.pipe(extractorstream);
	extractorstream.pipe(process.stdout);
}

function streamWarn(ctx, kind, err)
{
	console.error('warn: %s', err.message);
	if (ctx !== null)
		console.error('    at ' + ctx.label());
}

function ExtractorStream(opts, fieldnames)
{
	var streamoptions;

	streamoptions = mod_jsprim.mergeObjects(opts,
	    { 'objectMode': true }, { 'highWaterMark': 0 });
	mod_stream.Transform.call(this, streamoptions);
	mod_vstream.wrapTransform(this);

	this.es_indexes = null;
	this.es_types = null;
	this.es_fieldnames = fieldnames.slice(0);
}

mod_util.inherits(ExtractorStream, mod_stream.Transform);

ExtractorStream.prototype._transform = function (obj, _, callback)
{
	var self = this;
	var missing, invalid, values;

	if (typeof (obj) != 'object' || obj === null) {
		this.vsWarn(new Error('not an object'), 'not an object');
		setImmediate(callback);
		return;
	}

	if (this.es_indexes === null) {
		/* First row. */
		if (!obj.hasOwnProperty('keys') ||
		    !Array.isArray(obj['keys'])) {
			this.vsWarn(new Error('missing "keys"'),
			    'missing "keys"');
			callback(new VError('header row has no "keys"'));
			return;
		}

		this.es_types = [];
		this.es_indexes = this.es_fieldnames.map(function (f) {
			var lat = f.lastIndexOf('@');
			var type;
			if (lat != -1) {
				type = f.substr(lat + 1);
				f = f.substr(0, lat);
			} else {
				type = 'string';
			}

			self.es_types.push(type);
			return (obj['keys'].indexOf(f));
		});

		missing = this.es_fieldnames.filter(function (__, i) {
			return (self.es_indexes[i] == -1);
		});

		if (missing.length > 0) {
			callback(new VError('no such field(s): %s',
			    missing.join(', ')));
			return;
		}

		invalid = this.es_types.filter(function (t) {
			return (t != 'number' && t != 'string');
		});

		if (invalid.length > 0) {
			callback(new VError('unknown type(s): %s',
			    invalid.join(', ')));
		}

		setImmediate(callback);
		return;
	}

	if (!obj.hasOwnProperty('entry') || !Array.isArray(obj['entry'])) {
		this.vsWarn(new Error('missing "entry"'), 'missing "entry"');
		setImmediate(callback);
		return;
	}

	invalid = this.es_fieldnames.filter(function (__, i) {
		return (self.es_indexes[i] >= obj['entry'].length);
	});

	if (invalid.length > 0) {
		this.vsWarn(new Error('missing value'), 'missing value');
		setImmediate(callback);
		return;
	}

	values = this.es_indexes.map(function (i, j) {
		var rawvalue, type, value;

		rawvalue = obj['entry'][i];
		type = self.es_types[j];
		if (type == 'number') {
			value = Number(rawvalue);
			if (isNaN(value)) {
				return (new VError('bad number in field "%s"',
				    self.es_fieldnames[j]));
			}
		} else {
			value = rawvalue;
		}

		return (JSON.stringify(value));
	});

	invalid = values.filter(function (v) {
		return (v instanceof Error);
	});
	if (invalid.length > 0) {
		this.vsWarn(invalid[0], invalid[0].message);
		setImmediate(callback);
		return;
	}

	this.push(values.join(' ') + '\n');
	setImmediate(callback);
};

main();

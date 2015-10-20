/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2015, Joyent, Inc.
 */

/*
 * lib/common.js: common functions
 */

var VError = require('verror');

/* Public interface */
exports.readToEnd = readToEnd;
exports.readToEndJson = readToEndJson;

/*
 * readToEnd() and readToEndJson() have been reimplemented many times, and
 * should really be in a common library.
 */

function readToEnd(stream, callback)
{
	var data = '';

	stream.on('error', function (err) {
		callback(err);
	});

	stream.on('data', function (chunk) {
		data += chunk.toString('utf8');
	});

	stream.on('end', function () {
		callback(null, data);
	});
}

function readToEndJson(stream, callback)
{
	readToEnd(stream, function (err, data) {
		var obj;

		if (err) {
			callback(err);
			return;
		}

		try {
			obj = JSON.parse(data);
		} catch (ex) {
			callback(new VError(ex, 'invalid JSON'));
			return;
		}

		callback(null, obj);
	});
}

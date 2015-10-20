/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2015, Joyent, Inc.
 */

/*
 * lib/metering.js: working with Manta metering reports
 */

var mod_assertplus = require('assert-plus');
var mod_extsprintf = require('extsprintf');
var mod_jsprim = require('jsprim');
var mod_vasync = require('vasync');
var VError = require('verror');
var sprintf = mod_extsprintf.sprintf;

var mod_common = require('./common');

/* public interface */
exports.listMeteringReports = listMeteringReports;
exports.defaultMeteringRoot = '/poseidon/stor/usage';

/*
 * Fetches information about metering reports.  Named arguments in "args":
 *
 *     endDate (Date)	Scan reports up to and including the reports for
 *                      endDate, interpreted in UTC.  The time-of-day part of
 *                      this Date is ignored.
 *
 *     ndays (int > 0)	Scan reports for "ndays" leading up to endDate.
 *
 *     meteringRoot	Manta path to metering reports
 *     (string)		(Use default: this_module.defaultMeteringRoot)
 *
 *     concurrency	Maximum concurrency for Manta operations
 *     (number)
 *
 *     log		Bunyan logger
 *
 *     manta		Manta client
 *
 * callback() is invoked with the usual callback(err, result), where if there
 * was no error, then "result" is an object with methods for accessing
 * information about the reports that were found.  See ReportLister below.
 *
 * This method itself synchronously returns an object that may be useful in a
 * debugger to understand the current state.  You should not use this object
 * in any way (calling methods or reading or writing properties) from
 * JavaScript.
 */
function listMeteringReports(args, callback)
{
	var lister;

	lister = new ReportLister(args);
	lister.listMeteringReports(callback);
	return (lister);
}

function ReportLister(args)
{
	mod_assertplus.object(args, 'args');
	mod_assertplus.object(args.endDate, 'args.endDate');
	mod_assertplus.ok(args.endDate instanceof Date,
	    'args.endDate is a Date');
	mod_assertplus.number(args.ndays, 'args.ndays');
	mod_assertplus.ok(args.ndays > 0, 'args.ndays > 0');
	mod_assertplus.object(args.log, 'args.log');
	mod_assertplus.object(args.manta, 'args.manta');
	mod_assertplus.string(args.meteringRoot, 'args.meteringRoot');
	mod_assertplus.number(args.concurrency, 'args.concurrency');
	mod_assertplus.ok(args.concurrency > 0, 'args.concurrency > 0');

	/* configuration */
	this.mrl_end = new Date(args.endDate.getTime());
	this.mrl_end.setUTCHours(0, 0, 0, 0);
	this.mrl_ndays = args.ndays;
	this.mrl_log = args.log;
	this.mrl_manta = args.manta;
	this.mrl_meteringroot = args.meteringRoot;
	this.mrl_concurrency = args.concurrency;

	/* for debugging */
	this.mrl_queue = null;
	this.mrl_results = null;
	this.mrl_errors = null;
}

ReportLister.prototype.listMeteringReports = function (callback)
{
	var current, hour;
	var self = this;

	mod_assertplus.ok(this.mrl_queue === null,
	    'listMeteringReports() already called');

	/*
	 * Results are stored as an object mapping:
	 *
	 *     daily timestamp -> report kind -> details
	 *
	 * For report kinds "summary" and "storage", the "details" value is a
	 * count of records found, or "null" if no object was found.
	 *
	 * For report kinds "compute" and "request", the "details" value is a
	 * list of the per-hour objects that were present in Manta.
	 *
	 * This whole object will be post-processed to look for incomplete or
	 * missing reports.
	 */
	this.mrl_results = {};
	this.mrl_errors = [];

	/*
	 * Steps:
	 *
	 * For each day in the selected date range:
	 *
	 *   (1) Fetch /poseidon/stor/usage/summary/$date/d$day.json
	 *       and count lines.
	 *   (2) Fetch /poseidon/stor/usage/storage/$date/00/h00.json
	 *       and count lines
	 *   (3) Enumerate all of /poseidon/stor/usage/request/YYYY/MM/DD
	 *       and compare to what's expected.
	 *   (4) Enumerate all of /poseidon/stor/usage/compute/YYYY/MM/DD
	 *       and compare to what's expected.
	 *
	 * Now, every object expected as part of steps (3) and (4) is in a
	 * separate directory, so there's no way to avoid a Manta request per
	 * object that we expect.  As a result, the number of requests we have
	 * to make is well-known (and, unfortunately, high).  To manage
	 * concurrency, the easiest thing to do is pipe every request through a
	 * single queue with bounded concurrency and process each response
	 * independently as it comes in.
	 */
	this.mrl_queue = mod_vasync.queuev({
	    'concurrency': this.mrl_concurrency,
	    'worker': this.doMakeRequest.bind(this)
	});

	current = new Date(this.mrl_end.getTime());
	current.setUTCDate(current.getUTCDate() - this.mrl_ndays + 1);

	while (current.getTime() <= this.mrl_end.getTime() +
	    23 * 60 * 60 * 1000) {
		hour = new Date(current.getTime());

		if (current.getUTCHours() === 0) {
			this.mrl_results[hour.toISOString()] = {
			    'summary': null,
			    'storage': null,
			    'request': [],
			    'compute': []
			};

			this.mrl_queue.push({
			    'kind': 'summary',
			    'date': hour
			});

			this.mrl_queue.push({
			    'kind': 'storage',
			    'date': hour
			});
		}

		this.mrl_queue.push({
		    'kind': 'request',
		    'date': hour
		});

		this.mrl_queue.push({
		    'kind': 'compute',
		    'date': hour
		});

		current.setUTCHours(current.getUTCHours() + 1);
	}

	this.mrl_queue.on('end', function () {
		self.doFinishListing(callback);
	});

	this.mrl_queue.close();
};

/*
 * Make a metering request.  "rqinfo" has properties:
 *
 *     "kind"		One of "summary", "storage", "compute", or "request".
 *     			This is used to determine the path to the designated
 *     			report in Manta, as well as what to do with it.
 *
 *     "date"		Date for the report.
 *
 * This function translates this specification into an HTTP request to Manta,
 * makes the request, and saves the result into this.mrl_results.
 *
 * The "callback" here does not take any arguments.  It's only used to indicate
 * that this queue operation is complete.  Errors are reported via reportError()
 * below.
 */
ReportLister.prototype.doMakeRequest = function (rqinfo, callback)
{
	var path, method;
	var self = this;

	mod_assertplus.string(rqinfo.kind);
	mod_assertplus.date(rqinfo.date);

	path = sprintf('%s/%s/%04d/%02d/%02d', this.mrl_meteringroot,
	    rqinfo.kind, rqinfo.date.getUTCFullYear(),
	    rqinfo.date.getUTCMonth() + 1, rqinfo.date.getUTCDate());
	if (rqinfo.kind == 'summary') {
		/* The path is for a daily file. */
		method = 'get';
		path = sprintf('%s/d%02d.json', path, rqinfo.date.getUTCDate());
	} else {
		/*
		 * Although "storage" is only checked daily, the Manta objects
		 * are organized hourly.  For "storage", our caller will only
		 * enqueue requests where the hour is "00".
		 */
		if (rqinfo.kind == 'storage') {
			mod_assertplus.ok(rqinfo.date.getUTCHours() === 0);
			method = 'get';
		} else {
			method = 'head';
		}

		path = sprintf('%s/%02d/h%02d.json', path,
		    rqinfo.date.getUTCHours(), rqinfo.date.getUTCHours());
	}

	/*
	 * Attach the "method" and "path" to the request descriptor to allow us
	 * to print useful error messages in the event of failure.
	 */
	rqinfo.method = method;
	rqinfo.path = path;

	if (method == 'get') {
		this.mrl_manta.get(path, function (err, stream) {
			if (err) {
				self.recordError(rqinfo, err);
				callback();
				return;
			}

			mod_common.readToEnd(stream, function (err2, data) {
				var nlines;

				if (err2) {
					self.recordError(rqinfo, err2);
				} else {
					nlines = data.split(/\n/).length - 1;
					self.recordResult(rqinfo, nlines);
				}

				callback();
			});
		});
	} else {
		mod_assertplus.equal(method, 'head');
		this.mrl_manta.info(path, function (err, info) {
			if (err) {
				self.recordError(rqinfo, err);
				callback();
				return;
			}

			/*
			 * We don't actually care about the metadata or contents
			 * of this object, just whether it's present.
			 */
			self.recordResult(rqinfo);
			callback();
		});
	}
};

/*
 * Record errors that should be reported to the user.  Not that NotFoundErrors
 * (404s) are expected, and will be reported as missing data points.  This is
 * for unexpected operational errors, like other 400s (likely indicating a bug
 * in this program) or failures to contact the server.
 *
 * We log these to the bunyan log and then save them onto a global list so that
 * we can print these out to the user later.
 */
ReportLister.prototype.recordError = function (rqinfo, err)
{
	/* XXX These should be the same error. */
	if (err.name == 'ResourceNotFoundError' || err.name == 'NotFoundError')
		return;

	this.mrl_log.warn(err, 'request: error', rqinfo);
	/* XXX work around node-manta Errors' lack of messages */
	if (!err.message)
		err.message = err.name;
	rqinfo.error = new VError(err, '%s "%s"', rqinfo.method, rqinfo.path);
	this.mrl_errors.push(rqinfo);
};

/*
 * Record the results of one of the metering report requests.  See
 * doMakeRequest() above for how this works, and see the comment in
 * listMeteringReports() for how the results are structured.
 */
ReportLister.prototype.recordResult = function (rqinfo, count)
{
	var daydate, datekey, dateresults;

	daydate = new Date(rqinfo.date.getTime());
	daydate.setUTCHours(0, 0, 0, 0);
	datekey = daydate.toISOString();

	mod_assertplus.ok(this.mrl_results.hasOwnProperty(datekey));
	dateresults = this.mrl_results[datekey];
	mod_assertplus.ok(dateresults.hasOwnProperty(rqinfo.kind));

	if (rqinfo.kind == 'summary' || rqinfo.kind == 'storage') {
		/*
		 * For these two kinds, we should only make one request for a
		 * given day, so we must not have already seen a result.
		 */
		mod_assertplus.ok(dateresults[rqinfo.kind] === null);
		mod_assertplus.number(count);
		mod_assertplus.ok(count >= 0);
		dateresults[rqinfo.kind] = count;
		return;
	}

	/*
	 * For the other two kinds, we just keep track of the hours for which we
	 * have reports.
	 */
	mod_assertplus.ok(Array.isArray(dateresults[rqinfo.kind]));
	dateresults[rqinfo.kind].push(rqinfo.date.getUTCHours());
};

ReportLister.prototype.doFinishListing = function (callback)
{
	var self = this;
	var rv;

	rv = {
	    'byday': {},
	    'errors': this.mrl_errors.slice(0)
	};

	mod_jsprim.forEachKey(this.mrl_results,
	    function (daystamp, dayresults) {
		rv.byday[daystamp] = {
		    'summary': dayresults.summary,
		    'storage': dayresults.storage,
		    'compute': dayresults.compute.length,
		    'compute_missing': [],
		    'request': dayresults.request.length,
		    'request_missing': []
		};

		self.checkMissing('compute', dayresults.compute,
		    rv.byday[daystamp].compute_missing);
		self.checkMissing('request', dayresults.request,
		    rv.byday[daystamp].request_missing);
	    });

	callback(null, rv);
};

ReportLister.prototype.checkMissing = function (kind, kindresults, missing)
{
	var i, next, nexpected;

	nexpected = 24; /* hours */
	mod_assertplus.ok(Array.isArray(kindresults));
	if (kindresults.length == nexpected)
		return;

	kindresults = kindresults.slice().sort(
	    function (a, b) { return (a - b); });
	for (i = 0, next = 0; next < nexpected; next++) {
		if (i >= kindresults.length || kindresults[i] != next) {
			missing.push(next);
		} else {
			i++;
		}
	}
};

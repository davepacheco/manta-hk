/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2015, Joyent, Inc.
 */

/*
 * lib/dcaudit.js: manage directory count auditing jobs
 */

var mod_assertplus = require('assert-plus');
var mod_extsprintf = require('extsprintf');
var mod_fs = require('fs');
var mod_fstream = require('fstream');
var mod_jsprim = require('jsprim');
var mod_path = require('path');
var mod_stream = require('stream');
var mod_tar = require('tar');
var mod_vasync = require('vasync');
var VError = require('verror');
var mod_zlib = require('zlib');

var mod_common = require('./common');
var mod_dumps = require('./dumps');
var mod_dcaudit = require('./dcaudit');

var sprintf = mod_extsprintf.sprintf;

exports.launchDcAuditJob = launchDcAuditJob;
exports.listDcAuditJobs = listDcAuditJobs;
exports.reportDcAuditJob = reportDcAuditJob;
exports.defaultJobsListPath = '/poseidon/stor/manta_dcaudit/jobs.json';

var defaultAssetObject = '/poseidon/stor/manta_dcaudit/manta-hk-dcaudit.tgz';

/*
 * Kick off a Manta job to run a directory-count audit for the specified shard
 * and date.  Arguments include:
 *
 *    date (Date)	Audit directory counts using the database backups from
 *    			this date.
 *
 *    shard (string)	Name of the shard to audit
 *
 *    dumpRoot		See listDumps() in lib/dumps.js.
 *    (string)
 *
 *    jobsList		Path to object maintaining list of jobs.
 *    (string)		You probably want to use defaultJobsListPath.
 *
 *    log		Bunyan logger
 *
 *    manta		Manta client
 *
 *    [dryRun]		If true, creates the job, but adds no inputs so that
 *    (boolean)		it won't do anything.
 *
 * callback() is invoked with the usual callback(err, result) pattern, where if
 * there was no error, then "result" is an object with at least a "jobId" field
 * for the job that was kicked off.
 */
function launchDcAuditJob(args, callback)
{
	var op = {};
	var stages;

	mod_assertplus.object(args, 'args');
	mod_assertplus.date(args.date, 'args.date');
	mod_assertplus.string(args.shard, 'args.shard');
	mod_assertplus.string(args.dumpRoot, 'args.dumpRoot');
	mod_assertplus.string(args.jobsList, 'args.jobsList');
	mod_assertplus.object(args.log, 'args.log');
	mod_assertplus.object(args.manta, 'args.manta');
	mod_assertplus.func(callback, 'callback');
	mod_assertplus.optionalBool(args.dryRun, 'args.dryRun');

	/*
	 * Date of dumps to use for the audit.  We'll use this date to construct
	 * a directory path, and we assume dumps always happen at 00:00Z.
	 */
	op.aj_date = new Date(args.date.getTime());
	op.aj_date.setUTCHours(0, 0, 0, 0);

	/*
	 * Shard whose dumps to audit
	 * (e.g., "1.moray.us-east.joyent.us")
	 */
	op.aj_shard = args.shard;

	/*
	 * Manta directory where dumps are stored
	 * (e.g., "/poseidon/stor/manatee_backups")
	 */
	op.aj_dumproot = args.dumpRoot;

	/*
	 * Manta path where jobs list is stored, and associated data.
	 */
	op.aj_path_jobslist = args.jobsList;
	op.aj_jobslist = null;
	op.aj_jobslist_etag = null;

	/*
	 * Helper objects
	 */
	op.aj_log = args.log;		/* bunyan log */
	op.aj_manta = args.manta;	/* Manta client */
	op.aj_callback = args.callback;	/* user callback */

	/*
	 * Manta directory entry records for specific PostgreSQL table dumps.
	 */
	op.aj_entry_manta = null;	/* "manta" table */
	op.aj_entry_dircounts = null;	/* "manta_directory_counts" table */
	/* path to specific database dumps */
	op.aj_path_dumps = mod_dumps.dumpPathFor({
	    'dumpRoot': op.aj_dumproot,
	    'shard': op.aj_shard,
	    'timestamp': op.aj_date
	});

	/*
	 * Local assets
	 */
	op.aj_local_asset_root = mod_path.join(
	    __dirname, 'job-assets', 'dcaudit');
	op.aj_asset_object = defaultAssetObject;

	op.aj_jobpath = null;		/* created job id */

	stages = [
		stageCheckNodeModules,
		stageListShardFiles,
		stageCheckShardFiles,
		stageAssetUpload,
		stageJobCreate,
		stageJobsListFetch,
		stageJobsListUpdate
	];

	if (args.dryRun) {
		stages.push(stageJobDryRun);
	} else {
		stages.push(stageJobAddInput);
		stages.push(stageJobEndInput);
	}

	op.aj_pipeline = mod_vasync.pipeline({
	    'arg': op,
	    'funcs': stages
	}, callback);

	return (op.aj_pipeline);
}

function stageListShardFiles(aj, subcallback)
{
	var log = aj.aj_log;
	var path = aj.aj_path_dumps;

	log.debug({
	    'shard': aj.aj_shard,
	    'date': aj.aj_date,
	    'path': path
	}, 'listing unpacked objects');

	aj.aj_manta.ls(path, function (err, lister) {
		if (err) {
			subcallback(new VError(err, 'ls "%s"', path));
			return;
		}

		lister.on('entry', function (entry) {
			if (entry.type != 'object') {
				return;
			}

			if (!mod_jsprim.endsWith(entry.name, '.gz')) {
				return;
			}

			if (mod_jsprim.startsWith(entry.name, 'manta-')) {
				aj.aj_entry_manta = entry;
			} else if (mod_jsprim.startsWith(entry.name,
			    'manta_directory_counts-')) {
				aj.aj_entry_dircounts = entry;
			}
		});

		lister.on('end', function () {
			subcallback();
		});
	});
}

function stageCheckShardFiles(aj, subcallback)
{
	var log = aj.aj_log;

	if (aj.aj_entry_manta === null) {
		subcallback(new VError('"manta" table backup not found'));
		return;
	}

	if (aj.aj_entry_dircounts === null) {
		subcallback(new VError(
		    '"manta_directory_counts" table backup not found'));
		return;
	}

	log.debug({
	    'shard': aj.aj_shard,
	    'date': aj.aj_date
	}, 'found expected objects');

	subcallback();
}

function stageCheckNodeModules(aj, subcallback)
{
	var nodemodules = mod_path.join(aj.aj_local_asset_root, 'node_modules');
	var log = aj.aj_log;

	log.debug('checking for node_modules');
	mod_fs.stat(nodemodules, function (err) {
		if (err && err.code != 'ENOENT') {
			subcallback(new VError(err,
			    'accessing "%s"', nodemodules));
			return;
		}

		if (err) {
			subcallback(new VError(
			    'missing node_modules in asset ' +
			    'root (have you run "make"?)'));
		}

		subcallback();
	});
}

function stageAssetUpload(aj, subcallback)
{
	var dirstream, tar, gzip;
	var onError;
	var log = aj.aj_log;

	log.debug('uploading asset tarball');

	onError = function (which, err) {
		subcallback(new VError(err,
		    'uploading tarball: %s', which));
	};

	dirstream = mod_fstream.Reader({
	    'path': aj.aj_local_asset_root,
	    'type': 'Directory'
	});

	dirstream.on('error', onError.bind(null, 'fstream'));

	tar = mod_tar.Pack({
	    'fromBase': aj.aj_local_asset_root
	});
	tar.on('error', onError.bind(null, 'tar'));

	gzip = mod_zlib.createGzip();
	gzip.on('error', onError.bind(null, 'gzip'));

	dirstream.pipe(tar);
	tar.pipe(gzip);

	gzip.once('readable', function onGzipReadable() {
		log.debug('first gzip data');
		aj.aj_manta.put(aj.aj_asset_object, gzip,
		    { 'mkdirs': true }, function (err) {
			if (err) {
				onError('manta', err);
			} else {
				log.debug('PUT complete');
				subcallback();
			}
		    });
	});
}

function stageJobCreate(aj, subcallback)
{
	var jobdef;
	var dircount_object;

	dircount_object = aj.aj_path_dumps + '/' +
	    aj.aj_entry_dircounts['name'];

	jobdef = {};
	jobdef['name'] = 'manta-hk dircount-audit';
	jobdef['phases'] = [ {
	    'init': 'tar xzf "/assets' + aj.aj_asset_object + '"',
	    'memory': 8192,
	    'disk': 32,
	    'assets': [ dircount_object, aj.aj_asset_object ],
	    'exec': [
		'set -o pipefail',
		'set -o errexit',
		'set -o xtrace',
		'',
		'gzcat "$MANTA_INPUT_FILE" | \\',
		'    node pgjxfields.js dirname | sort | \\',
		'    uniq -c > raw_counts.txt',
		'gzcat "/assets' + dircount_object + '" | \\',
		'    node pgjxfields.js entries@number _key | \\',
		'    sort -k2 > reported_counts.txt',
		'ls -l *.txt',
		'node auditcounts.js -v raw_counts.txt reported_counts.txt'
	    ].join('\n')
	} ];

	aj.aj_manta.createJob(jobdef, function (err, jobPath) {
		if (err) {
			subcallback(new VError(err, 'creating job'));
			return;
		}

		aj.aj_jobpath = jobPath;
		console.error('created job %s', jobPath);
		subcallback();
	});
}

function stageJobsListFetch(aj, subcallback)
{
	fetchRawJobsList({
	    'log': aj.aj_log,
	    'manta': aj.aj_manta,
	    'jobsList': aj.aj_path_jobslist
	}, function (err, obj, etag) {
		if (err) {
			subcallback(err);
		} else {
			aj.aj_jobslist = obj;
			aj.aj_jobslist_etag = etag;
			subcallback();
		}
	});
}

function stageJobsListUpdate(aj, subcallback)
{
	var updated, stream, path, options;

	mod_assertplus.ok(aj.aj_jobslist !== null);
	updated = mod_jsprim.deepCopy(aj.aj_jobslist);
	if (!updated.hasOwnProperty('jobs')) {
		updated['jobs'] = [];
	}

	updated['jobs'].push({
	    'created': new Date().toISOString(),
	    'jobPath': aj.aj_jobpath,
	    'inputDate': aj.aj_date.toISOString(),
	    'inputShard': aj.aj_shard
	});

	stream = new mod_stream.PassThrough();
	path = aj.aj_path_jobslist;
	options = {
	    'mkdirs': true,
	    'headers': {
		'if-match': aj.aj_jobslist_etag === null ?
		    '' : aj.aj_jobslist_etag
	    }
	};

	aj.aj_manta.put(path, stream, options, function (err) {
		if (err) {
			err = new VError(err, 'updating "%s"', path);
		}

		subcallback(err);
	});

	stream.end(JSON.stringify(updated));
}

function stageJobDryRun(aj, subcallback)
{
	console.error('stopping here due to dry run');
	subcallback();
}

function stageJobAddInput(aj, subcallback)
{
	var path = aj.aj_path_dumps + '/' + aj.aj_entry_manta['name'];
	aj.aj_manta.addJobKey(aj.aj_jobpath, path, function (err) {
		if (err) {
			err = new VError(err, 'adding input');
		}

		subcallback(err);
	});
}

function stageJobEndInput(aj, subcallback)
{
	aj.aj_manta.endJob(aj.aj_jobpath, function (err) {
		if (err) {
			err = new VError(err, 'ending input');
		}

		subcallback(err);
	});
}

/*
 * List recently-run directory-count audit jobs.
 *
 *     log		Bunyan logger
 *
 *     manta		Manta client
 *
 *     jobsList		Path to object maintaining list of jobs.
 *     			You probably want to use defaultJobsListPath.
 *
 * "callback" is invoked as callback(error, obj, etag).  If "error" is non-null,
 * the other arguments are not defined.  Otherwise, "obj" will be an object, and
 * "etag" MAY be non-null.  "etag" denotes the etag on the HTTP resource
 * representing the jobs list.  If "obj" is valid but "etag" is null, then the
 * jobs list was presumed empty.
 */
function fetchRawJobsList(args, callback)
{
	var path, manta;

	mod_assertplus.object(args, 'args');
	mod_assertplus.object(args.log, 'args.log');
	mod_assertplus.object(args.manta, 'args.manta');
	mod_assertplus.string(args.jobsList, 'args.jobsList');
	mod_assertplus.func(callback, 'callback');

	manta = args.manta;
	path = args.jobsList;
	manta.get(path, function (err, stream, response) {
		var etag;

		if (err && err.name != 'NotFoundError' &&
		    err.name != 'ResourceNotFoundError') {
			callback(new VError(err, 'fetching "%s"', path));
			return;
		}

		if (err) {
			callback(null, { 'jobs': [] }, null);
			return;
		}

		etag = response.headers['etag'];
		mod_common.readToEndJson(stream, function (err2, obj) {
			if (err2) {
				callback(new VError(
				    err2, 'fetch/parse "%s"', path));
				return;
			}

			callback(null, obj, etag);
		});
	});

}

/*
 * List recently-run directory-count audit jobs.
 *
 *     log		Bunyan logger
 *
 *     manta		Manta client
 *
 *     jobsList		Path to object maintaining list of jobs.
 *     			You probably want to use defaultJobsListPath.
 */
function listDcAuditJobs(args, callback)
{
	var log, manta;

	mod_assertplus.object(args, 'args');
	mod_assertplus.object(args.log, 'args.log');
	mod_assertplus.object(args.manta, 'args.manta');
	mod_assertplus.string(args.jobsList, 'args.jobsList');
	mod_assertplus.func(callback, 'callback');

	log = args.log;
	manta = args.manta;

	fetchRawJobsList(args, function (err, obj) {
		var queue;

		if (err) {
			callback(err);
			return;
		}

		queue = mod_vasync.queuev({
		    'concurrency': 10,
		    'worker': function queueFetchJobState(job, subcallback) {
			fetchJobState({
			    'jobPath': job['jobPath'],
			    'log': log,
			    'manta': manta
			}, function (err2, result) {
				job['error'] = err2;
				job['state'] = result;
				subcallback();
			});
		    }
		});
		queue.on('end', function () {
			callback(null, obj);
		});

		obj['jobs'].forEach(
		    function enqueueJob(job) { queue.push(job); });
		queue.close();
	});
}

/*
 * Fetch a job's state.  This interface really should be provided by node-manta,
 * but it's not currently.  Named arguments:
 *
 *     log		bunyan logger
 *
 *     manta		Manta client
 *
 *     jobPath		either a jobId or a Manta path to a jobs directory
 */
function fetchJobState(args, callback)
{
	var manta, jobPath;

	mod_assertplus.object(args, 'args');
	mod_assertplus.object(args.log, 'args.log');
	mod_assertplus.object(args.manta, 'args.manta');
	mod_assertplus.string(args.jobPath, 'args.jobPath');
	mod_assertplus.func(callback, 'callback');

	/*
	 * Check the live status first.
	 */
	manta = args.manta;
	jobPath = args.jobPath;
	manta.job(jobPath, function (err, result) {
		if (!err) {
			callback(null, result);
			return;
		}

		if (err['name'] != 'NotFoundError' &&
		    err['name'] != 'ResourceNotFoundError') {
			callback(new VError(err, 'fetch job "%s"', jobPath));
			return;
		}

		/*
		 * There's no live status, but this may be an archived job with
		 * a "job.json" object containing the details.
		 */
		var statusPath = jobObjectPath(jobPath, 'job.json');
		manta.get(statusPath, function (err2, stream) {
			if (err2) {
				callback(new VError(err2,
				    'fetch "%s"', jobPath));
				return;
			}

			mod_common.readToEndJson(stream, function (err3, obj) {
				if (err3) {
					callback(new VError(err3,
					    'parse "%s"', jobPath));
					return;
				}

				callback(null, obj);
			});
		});
	});
}

/*
 * Given a jobPath that may be a simple string id or a path to a jobs directory,
 * and an object name (like "job.json" or "out.txt"), return the full Manta path
 * to that object for that job.
 */
function jobObjectPath(jobPath, obj)
{
	if (jobPath.indexOf('/') == -1) {
		jobPath = [ '/poseidon/jobs', jobPath ].join('/');
	}

	return ([ jobPath, obj ].join('/'));
}

/*
 * Fetches detailed information about a directory-count audit job.
 */
function reportDcAuditJob(args, callback)
{
	mod_assertplus.object(args, 'args');
	mod_assertplus.object(args.log, 'args.log');
	mod_assertplus.object(args.manta, 'args.manta');
	mod_assertplus.string(args.jobPath, 'args.jobPath');
	mod_assertplus.func(callback, 'callback');

	/*
	 * Some of the complexity in this pipeline should be abstracted in
	 * node-manta.  Regrettably, it's not.  As a result, we have to
	 * distinguish here between archived jobs and not, and we hardcode the
	 * paths.
	 */
	var r = {};
	r.r_log = args.log;
	r.r_manta = args.manta;
	r.r_jobpath = args.jobPath;
	r.r_callback = args.callback;
	r.r_jobstate = null;		/* full job state object */
	r.r_output_path = null;		/* path to output object */
	r.r_object = null;		/* stream for output object */
	r.r_pipeline = mod_vasync.pipeline({
	    'arg': r,
	    'funcs': [
		reportFetchJobState,
		reportCheckJobState,
		reportFetchJobOutputs,
		reportFetchJobOutputContents
	    ]
	}, function (err) {
		if (err) {
			callback(err);
			return;
		}

		callback(null, {
		    'rawStream': args.r_object
		});
	});

	return (r.r_pipeline);
}

function reportFetchJobState(r, callback)
{
	fetchJobState({
	    'log': r.r_log,
	    'manta': r.r_manta,
	    'jobPath': r.r_jobpath
	}, function (err, result) {
		if (err) {
			callback(err);
			return;
		}

		r.r_jobstate = result;
		callback();
	});
}

function reportCheckJobState(r, callback)
{
	var js, stats;

	js = r.r_jobstate;

	if (js['state'] != 'done') {
		callback(new VError('job "%s": still running',
		    r.r_jobpath));
		return;
	}

	if (!js.hasOwnProperty('timeArchiveDone')) {
		callback(new VError('job "%s": not yet archived',
		    r.r_jobpath));
		return;
	}

	stats = js['stats'];
	if (stats['errors'] !== 0) {
		callback(new VError('job "%s": emitted %d error%s',
		    r.r_jobpath, stats['errors'],
		    stats['errors'] == 1 ? '' : 's'));
		return;
	}

	if (stats['outputs'] !== 1) {
		callback(new VError('job "%s": wrong number ' +
		    'of outputs emitted (expected 1, found %d)',
		    r.r_jobpath, stats['outputs']));
		return;
	}

	callback();
}

function reportFetchJobOutputs(r, callback)
{
	var outpath;

	outpath = jobObjectPath(r.r_jobpath, 'out.txt');
	r.r_manta.get(outpath, function (err, stream) {
		if (err) {
			callback(new VError(err, 'fetch "%s"', outpath));
			return;
		}

		mod_common.readToEnd(stream, function (err2, data) {
			var lines;

			if (!err2) {
				lines = data.split('\n');
				if (lines.length != 2 || lines[1] != '') {
					err2 = new VError(
					    'unexpected format for "%s"',
					    outpath);
				} else {
					r.r_output_path = lines[0];
				}
			}

			if (err2) {
				callback(err2);
				return;
			}

			callback();
		});
	});
}

function reportFetchJobOutputContents(r, callback)
{
	mod_assertplus.string(r.r_output_path);
	r.r_manta.get(r.r_output_path, function (err, stream) {
		if (err) {
			callback(new VError(
			    err, 'fetch "%s"', r.r_output_path));
			return;
		}

		r.r_object = stream;
		callback();
	});
}

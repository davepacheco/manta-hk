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

var mod_dumps = require('./dumps');
var mod_dcaudit = require('./dcaudit');

var sprintf = mod_extsprintf.sprintf;

exports.launchDcAuditJob = launchDcAuditJob;
exports.listDcAuditJobs = listDcAuditJobs;
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
 *    jobsList		Path to file maintaining list of jobs.
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
	var path = aj.aj_path_jobslist;

	aj.aj_manta.get(path, function (err, stream, response) {
		if (err && err.name != 'NotFoundError' &&
		    err.name != 'ResourceNotFoundError') {
			subcallback(new VError(err, 'fetching "%s"', path));
			return;
		}

		if (err) {
			aj.aj_jobslist = {};
			subcallback();
			return;
		}

		aj.aj_jobslist_etag = response.headers['etag'];

		var data = '';
		stream.on('data', function (chunk) {
			data += chunk.toString('utf8');
		});
		stream.on('end', function () {
			var obj;

			try {
				obj = JSON.parse(data);
			} catch (ex) {
				subcallback(new VError(
				    ex, 'parsing "%s"', path));
				return;
			}

			aj.aj_jobslist = obj;
			subcallback();
		});
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


function listDcAuditJobs(args, callback)
{
	setImmediate(callback, new Error('not yet implemented'));
}

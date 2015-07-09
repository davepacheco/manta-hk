/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2015, Joyent, Inc.
 */

/*
 * lib/dumps.js: working with Manatee database dumps
 */

var mod_assertplus = require('assert-plus');
var mod_extsprintf = require('extsprintf');
var mod_jsprim = require('jsprim');
var mod_strsplit = require('strsplit');
var mod_vasync = require('vasync');
var VError = require('verror');
var sprintf = mod_extsprintf.sprintf;

/* public interface */
exports.listDumps = listDumps;
exports.defaultDumpRoot = '/poseidon/stor/manatee_backups';

/*
 * Fetches information about database dumps and their associated unpacked
 * objects.  Named arguments in "args":
 *
 *     endDate (Date)	Scan dumps up to and including the dumps for endDate,
 *     			interpreted in UTC.  The time-of-day part of this Date
 *     			is ignored.
 *
 *     ndays (int > 0)	Scan dumps for "ndays" leading up to endDate.
 *
 *     shards (optional	Only examine dumps for named shards.  The default is
 *     array of string)	to show dump information for all shards.
 *
 *     dumpRoot		Manta path to database dumps
 *     (string)		(Use default: this_module.defaultDumpRoot)
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
 * information about the dumps that were found.  See DumpLister below.
 *
 * This method itself synchronously returns an object that may be useful in a
 * debugger to understand the current state.  You should not use this object
 * in any way (calling methods or reading or writing properties) from
 * JavaScript.
 */
function listDumps(args, callback)
{
	var lister;

	lister = new DumpLister(args);
	lister.listDumps(function (err) { callback(err, lister); });
	return (lister);
}

function DumpLister(args)
{
	mod_assertplus.object(args, 'args');
	mod_assertplus.object(args.endDate, 'args.endDate');
	mod_assertplus.ok(args.endDate instanceof Date,
	    'args.endDate is a Date');
	mod_assertplus.number(args.ndays, 'args.ndays');
	mod_assertplus.ok(args.ndays > 0, 'args.ndays > 0');
	if (args.shards !== null) {
		mod_assertplus.arrayOfString(args.shards, 'args.shards');
	}
	mod_assertplus.object(args.log, 'args.log');
	mod_assertplus.object(args.manta, 'args.manta');
	mod_assertplus.string(args.dumpRoot, 'args.dumpRoot');
	mod_assertplus.number(args.concurrency, 'args.concurrency');
	mod_assertplus.ok(args.concurrency > 0, 'args.concurrency > 0');

	/* configuration */
	this.dl_end = new Date(args.endDate.getTime());
	this.dl_end.setUTCHours(0, 0, 0, 0);
	this.dl_ndays = args.ndays;
	this.dl_log = args.log;
	this.dl_manta = args.manta;
	this.dl_shards = args.shards === null ? null : args.shards.slice(0);
	this.dl_dumproot = args.dumpRoot;
	this.dl_concurrency = args.concurrency;

	/* vasync queue for fetching a single dump */
	this.dl_queue = mod_vasync.queuev({
	    'concurrency': this.dl_concurrency,
	    'worker': this.doFetchShardDumpsForDay.bind(this)
	});

	/* dumps, indexed by date timestamp and then by shard name */
	this.dl_dumps = {};

	/* vasync waterfall (for debugging) */
	this.dl_waterfall = null;

	/* list of shard names found in Manta */
	this.dl_foundshards = [];

	/* accumulated errors */
	this.dl_errors = [];

	/* start and completion times */
	this.dl_start = null;
	this.dl_done = null;
}

DumpLister.prototype.listDumps = function (callback)
{
	mod_assertplus.ok(this.dl_waterfall === null,
	    'listDumps() already called');

	var self = this;
	this.dl_start = new Date();
	this.dl_waterfall = mod_vasync.waterfall([
	    function listAllShards(wfcallback) {
		/*
		 * List all the shards for which there are any dumps.
		 */
		self.dl_manta.ls(self.dl_dumproot, function (err, lister) {
			if (err) {
				wfcallback(new VError(err,
				    'failed to list shards at "%s"',
				    self.dl_dumproot));
				return;
			}

			lister.on('entry', function (entry) {
				if (entry.type == 'directory')
					self.dl_foundshards.push(entry.name);
			});

			lister.on('end', function () { wfcallback(); });
		});
	    },

	    function selectShards(wfcallback) {
		/*
		 * Of the shards we found, pick out the ones that the user asked
		 * for.  It's an error if any of these don't exist.  It's also
		 * an error if we're left with zero shards.
		 */
		self.dl_log.debug(self.dl_foundshards, 'found shards');
		if (self.dl_shards === null) {
			self.dl_log.debug('using %d found shards',
			    self.dl_foundshards.length);
			self.dl_shards = self.dl_foundshards;
			wfcallback();
			return;
		}

		var sets = setCompare(self.dl_shards, self.dl_foundshards);
		if (sets.lhsonly.length > 0) {
			wfcallback(new VError('requested shard%s not found: ' +
			    '"%s"', sets.lhsonly.length == 1 ? '' : 's',
			    sets.lhsonly.join(', ')));
			return;
		}

		if (sets.isect.length === 0) {
			wfcallback(new VError('no shards selected'));
			return;
		}

		self.dl_shards = sets.isect.slice(0);
		wfcallback();
	    },

	    function enumDirectories(wfcallback) {
		var current = new Date(self.dl_end.getTime());
		current.setUTCDate(current.getUTCDate() - self.dl_ndays + 1);

		while (current.getTime() <= self.dl_end.getTime()) {
			self.dl_shards.forEach(function (shard) {
				self.dl_queue.push({
				    'shard': shard,
				    'date': new Date(current.getTime())
				});
			});

			current.setUTCDate(current.getUTCDate() + 1);
		}

		self.dl_queue.on('end', function () { wfcallback(); });
		self.dl_queue.close();
	    }
	], function (err) {
		self.dl_done = new Date();

		if (!err && self.dl_errors.length > 0) {
			err = new VError(self.dl_errors[0], 'first error');
		}

		if (err) {
			callback(new VError(err, 'failed to list dumps'));
		} else {
			callback(null, self);
		}
	});
};

/*
 * Fetch information about database dumps for the given shard and date.
 * "arg" has named properties:
 *
 *     shard (string)	shard name
 *     date (Date)	date of dump to look for
 *
 * Results are stored into this.dl_dumps, which is organized as:
 *
 *     date -> shard name -> {
 *	    path (string):  full path to the dump
 *	    name (string):  basename of dump object
 *	    size (number):  size (in bytes) of the dump
 *	    sizemb (num):   size (in megabytes) of the dump
 *	    mtime (Date):   time that the dump was uploaded
 *	    stime (Date):   time that the dump appears to have started
 *	    etime (number): time (in milliseconds) that the dump operation
 *			    appeared to take
 *	    ended (number): time after the hour when the dump was available
 *			    (hours, minutes, seconds, ms part of "mtime")
 *	    unpacked (bool): whether the dump appears to have been unpacked
 *	    objects (array of names): list of unpacked objects
 *	    waslate (bool):  indicates whether this dump was likely too late
 *			     for normal processing
 *     }
 */
DumpLister.prototype.doFetchShardDumpsForDay = function (arg, callback)
{
	var self, when, whenkey, path;

	/*
	 * We currently only look for dumps at the 00 hour, since that's what
	 * Manta does automatically.  It may be useful to extend this to search
	 * for more dumps and notify if they're found.
	 */
	mod_assertplus.string(arg.shard, 'arg.shard');
	mod_assertplus.object(arg.date, 'arg.date');
	mod_assertplus.equal(arg.date.getUTCHours(), 0);

	self = this;
	when = arg.date;
	whenkey = when.toISOString();
	path = [
	    self.dl_dumproot,
	    arg.shard,
	    sprintf('%4d', when.getUTCFullYear()),
	    sprintf('%02d', when.getUTCMonth() + 1),
	    sprintf('%02d', when.getUTCDate()),
	    sprintf('%02d', when.getUTCHours())
	].join('/');

	if (!self.dl_dumps.hasOwnProperty(whenkey))
		self.dl_dumps[whenkey] = {};

	self.dl_manta.ls(path, function (err, emitter) {
		if (err && err.name == 'NotFoundError') {
			self.dl_log.warn('no dump found', { 'path': path });
			callback();
			return;
		}

		if (err) {
			self.dl_log.error(err, 'ls ' + path);
			self.dl_errors.push(new VError(err, 'ls "%s"', path));
			callback();
			return;
		}

		var dump = null;
		var unpacked = [];
		emitter.on('entry', function (entry) {
			var what;

			what = parseDumpDirent(when, self.dl_log, path, entry);
			if (what === null) {
				unpacked.push(entry.name);
				return;
			}

			/*
			 * If the encoded date is more than a day off, there's
			 * something wrong.
			 */
			if (Math.abs(what.stime.getTime() - when.getTime()) >
			    86400000) {
				self.dl_log.warn(
				    { 'path': path, 'name': entry.name },
				    'garbled date in name (too far off)');
				return;
			}

			if (dump !== null) {
				self.dl_log.warn('directory "%s": found ' +
				    'unexpected match "%s" (already saw "%s")',
				    path, entry.name, dump.name);
				return;
			}

			dump = what;
		});

		emitter.on('end', function () {
			if (dump === null) {
				self.dl_log.warn(
				    'directory "%s": no dump found', path);
			} else {
				mod_assertplus.ok(!self.dl_dumps[
				    whenkey].hasOwnProperty(arg.shard));
				self.dl_dumps[whenkey][arg.shard] = dump;
				dump.unpacked = unpacked.length > 0;
				dump.objects = unpacked;
				dump.waslate = dump.ended >= 120 * 60 * 1000;
			}

			callback();
		});
	});
};

/*
 * Given the path to a dump directory (including shard name and date
 * components) and a directory entry from that directory (as returned by
 * manta.ls(), return an object describing the dump.  If the dirent does not
 * represent a Manatee dump, return null.
 */
function parseDumpDirent(start, log, path, entry)
{
	var dateparts, s, starttime, dump;

	/*
	 * We're looking for objects whose names look like:
	 *
	 *     moray-YYYY-mm-dd-HH-mm-ss.gz
	 *
	 * Anything that doesn't match this pattern should be ignored.  There
	 * should be exactly one file in each directory that looks like this.
	 * Our caller will issue a warning if it finds more than one.
	 */
	if (entry.type != 'object')
		return (null);

	if (!mod_jsprim.startsWith(entry.name, 'moray-'))
		return (null);

	if (!mod_jsprim.endsWith(entry.name, '.gz'))
		return (null);

	/*
	 * Select only the date component and parse it.
	 */
	s = entry.name.substr('moray-'.length);
	s = s.substr(0, s.length - '.gz'.length);
	dateparts = mod_strsplit(s, '-', 6);
	if (dateparts.length != 6) {
		log.warn({ 'path': path, 'name': entry.name },
		    'garbled date in name (wrong format)');
		return (null);
	}

	starttime = new Date(Date.UTC(dateparts[0],
	    dateparts[1] - 1, dateparts[2], dateparts[3],
	    dateparts[4], dateparts[5]));
	if (isNaN(starttime.getTime())) {
		log.warn({ 'path': path, 'name': entry.name },
		    'garbled date in name (date parse)');
		return (null);
	}

	dump = {
	    'path': path + '/' + entry.name,
	    'name': entry.name,
	    'size': entry.size,
	    'sizemb': Math.ceil(entry.size / 1024 / 1024),
	    'stime': starttime,
	    'mtime': new Date(entry.mtime)
	};

	dump.etime = dump.mtime.getTime() - dump.stime.getTime();
	dump.ended = dump.mtime.getTime() - start.getTime();
	return (dump);
}


/*
 * Compares the values of two lists of strings.  Returns an object with:
 *
 *     isect	intersection (list of entries common to both lists)
 *
 *     lhsonly	list of entries in only the first set
 *
 *     rhsonly	list of entries in only the second set
 *
 * Lists are treated as sets, so duplicates are ignored.
 */
function setCompare(lhs, rhs)
{
	var lhsidx, rv;

	lhsidx = {};
	rv = {
	    'isect': [],
	    'lhsonly': [],
	    'rhsonly': []
	};

	lhs.forEach(function (s) { lhsidx[s] = true; });
	rhs.forEach(function (s) {
		if (lhsidx.hasOwnProperty(s))
			rv.isect.push(s);
		else
			rv.rhsonly.push(s);
	});

	/*
	 * This could be more efficient if these sets ever got large.
	 */
	rv.lhsonly = lhs.filter(function (s) {
		return (rv.isect.indexOf(s) == -1);
	});

	return (rv);
}

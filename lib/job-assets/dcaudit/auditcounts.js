/*
 * auditcounts.js: given two sorted input files representing directory counts,
 * report differences between them.  This programs streams through both files to
 * avoid ever buffering too much data.
 */

var mod_assertplus = require('assert-plus');
var mod_cmdutil = require('cmdutil');
var mod_jsprim = require('jsprim');
var mod_fs = require('fs');
var mod_lstream = require('lstream');
var mod_stream = require('stream');
var mod_strsplit = require('strsplit');
var mod_util = require('util');
var mod_vstream = require('vstream');
var VError = require('verror');

var nwarnings, cmpstream;
var done = false;

/*
 * To minimize the memory footprint, the guts of this program are implemented
 * using object-mode streams.  This is made possible because this program
 * assumes (and verifies) that the inputs are sorted.  The pipelines look like
 * this:
 *
 *     input 1                                          input 2
 *     file read stream     (read file)                 file read stream
 *         |                                               |
 *         | (pipe)                                        | (pipe)
 *         v                                               v
 *     lstream              (break text into lines)     lstream
 *         |                                               |
 *         | (pipe)                                        | (pipe)
 *         v                                               v
 *     splitstream          (parse lines into           splitstream
 *         |                <count,dirname> tuples)        |
 *         |                                               |
 *         +-------------------+       +-------------------+
 *                             |       |
 *                             v       v
 *                             joinstream       (march through inputs, identify
 *                                 |             mis-sorts, and join records
 *                                 | (pipe)      from both streams)
 *                                 v
 *                             auditstream      (emit summary of correctness of
 *                                 |            the counts for each directory)
 *                                 | (pipe)
 *                                 v
 *                            process.stdout
 *
 * Nearly all of these streams are synchronous transforms with a highWaterMark
 * of zero.  The only data buffering should happen in the file read streams and
 * the lstreams.  All of these implement backpressure to avoid blowing out
 * memory usage.
 */
function main()
{
	var filenames, sources;
	var joinstream;
	var verbose;

	process.on('exit', function (code) {
		if (code === 0 && !done) {
			console.error('ERROR: premature exit');
			sources[0].s_fstream.vsHead().vsWalk(function (s) {
				s.vsDumpDebug(process.stderr);
			});
			console.error('----');
			sources[1].s_fstream.vsHead().vsWalk(function (s) {
				s.vsDumpDebug(process.stderr);
			});
			console.error('----');
			cmpstream.vsHead().vsWalk(function (s) {
				s.vsDumpDebug(process.stderr);
			});
			process.abort();
		}
	});

	nwarnings = 0;
	mod_cmdutil.configure({
	    'synopses': [ 'MANTA_SUMMARY DIRCOUNT_SUMMARY' ],
	    'usageMessage': 'merge and summarize raw and ' +
	        'reported directory counts'
	});

	filenames = process.argv.slice(2);
	if (filenames.length > 0 && filenames[0] == '-v') {
		verbose = true;
		filenames.shift();
	} else {
		verbose = false;
	}

	if (filenames.length != 2) {
		done = true;
		mod_cmdutil.usage();
	}

	sources = filenames.map(function (filename) {
		var fstream, lstream, splitstream;

		fstream = mod_vstream.wrapStream(
		    mod_fs.createReadStream(filename));
		lstream = mod_vstream.wrapStream(new mod_lstream({
		    'highWaterMark': 128
		}));
		splitstream = new SplitStream();

		fstream.pipe(lstream);
		lstream.pipe(splitstream);

		splitstream.on('warn', streamWarn);

		return ({
		    's_filename': filename,
		    's_fstream': fstream,
		    's_lstream': lstream,
		    's_splitstream': splitstream
		});
	});

	joinstream = new JoinStream({
	    'joinOnIndex': 1,
	    'sources': sources.map(function (s) { return (s.s_splitstream); })
	});
	joinstream.on('error', mod_cmdutil.fail);

	cmpstream = new AuditStream({ 'verbose': verbose });

	joinstream.pipe(cmpstream);
	cmpstream.pipe(process.stdout);
	cmpstream.on('end', endReport);
}

function streamWarn(ctx, kind, err)
{
	nwarnings++;
	console.error('warn: %s', err.message);
	if (ctx !== null)
		console.error('    at ' + ctx.label());
}

function endReport()
{
	console.error('memory usage: ', process.memoryUsage());
	console.error('entries processed: ', cmpstream.nprocessed());
	console.error('warnings: ', nwarnings);
	done = true;
	if (nwarnings > 0)
		process.exit(1);
}

/*
 * Object-mode transform stream that parses lines of the form:
 *
 *     [<whitespace>] <digits> <whitespace> <JSON-encoded string>
 *
 * This is similar to the output of "sort | uniq -c", where the rows input to
 * "sort" are JSON-encoded strings.
 *
 * Objects output from this stream are arrays with two elements: the count, and
 * the JSON-decoded string.
 *
 * Operational errors are emitted as warnings via node-vstream.  They include:
 *
 *     o an input line doesn't match the above form at all
 *     o the first field is not a valid integer
 *     o the second field is not parseable as a JSON string
 */
function SplitStream(options)
{
	var streamoptions = mod_jsprim.mergeObjects(options,
	    { 'objectMode': true }, { 'highWaterMark': 0 });
	mod_stream.Transform.call(this, streamoptions);
	mod_vstream.wrapTransform(this);
}

mod_util.inherits(SplitStream, mod_stream.Transform);

SplitStream.prototype._transform = function (line, _, callback)
{
	var parts, count, str;

	line = line.trim();
	parts = mod_strsplit(line, /\s+/, 2);
	if (parts.length != 2) {
		this.vsWarn(new Error('line garbled'), 'line garbled');
		setImmediate(callback);
		return;
	}

	/* XXX want common function for parsing an integer */
	count = parseInt(parts[0], 10);
	if (isNaN(count) || parts[0].toString() != count) {
		this.vsWarn(new Error('field 1 is not an integer'),
		    'not an integer');
		setImmediate(callback);
		return;
	}

	/*
	 * Parse the string as JSON in order to validate it, but internally, we
	 * use the raw representation.  This ensures that we treat these as
	 * opaque tokens.  Additionally, we only know that the serialized
	 * representations are sorted, not the parsed representations.  The
	 * result is different for strings "A B" and "A", where:
	 *
	 *     serialized: '"A"' comes after '"A B"', but
	 *     parsed:     'A' comes before 'A B'
	 */
	try {
		str = JSON.parse(parts[1]);
	} catch (err) {
		this.vsWarn(new VError(err, 'invalid JSON'), 'invalid JSON');
		setImmediate(callback);
		return;
	}

	if (typeof (str) != 'string') {
		this.vsWarn(new Error('field 2 is not a string'),
		    'not a string');
		setImmediate(callback);
		return;
	}

	/* See the comment above. */
	this.push([ count, parts[1] ]);
	setImmediate(callback);
	return;
};

/*
 * Joins N SplitStreams, which are assumed to be sorted by the join key.  Emits
 * joined objects of the form:
 *
 *     [ join key, value from stream 1, value from stream 2, ... ]
 *
 * Named arguments include:
 *
 *     joinOnIndex	integer, index of each input on which to join
 *
 *     sources		list of streams to read from.  Each stream should emit
 *     			an array that has the joinOnIndex value.
 *
 *     [streamOptions]	options to pass to stream constructor
 */
function JoinStream(args)
{
	var streamoptions;
	var self = this;

	mod_assertplus.object(args, 'args');
	mod_assertplus.number(args.joinOnIndex, 'args.joinOnIndex');
	mod_assertplus.arrayOfObject(args.sources, 'args.sources');
	mod_assertplus.optionalObject(args.streamOptions, 'args.streamOptions');
	mod_assertplus.ok(args.sources.length > 0);

	streamoptions = mod_jsprim.mergeObjects(args.streamOptions,
	    { 'objectMode': true }, { 'highWaterMark': 0 });
	mod_stream.Readable.call(this, streamoptions);
	mod_vstream.wrapStream(this);

	this.js_joinidx = args.joinOnIndex;
	this.js_waiting = false;
	this.js_working = false;
	this.js_sources = args.sources.map(function (s) {
		var src = {
		    'src_stream': s,
		    'src_next': null,
		    'src_next_join': null,
		    'src_last_join': null,
		    'src_done': false
		};

		s.on('readable', function sourceReadable() {
			self.kickIfWaiting();
		});

		s.on('end', function sourceEnded() {
			src.src_done = true;
			self.kickIfWaiting();
		});

		return (src);
	});
}

mod_util.inherits(JoinStream, mod_stream.Readable);

/*
 * Initially, js_waiting is false.  js_waiting becomes true when we run out of
 * data from our upstream sources, but we still have room to emit more data
 * ourselves.  When an upstream source becomes readable or ended AND js_waiting
 * is set, then js_waiting is cleared and we read as much as possible and push
 * until either we block again (and js_waiting is set again) or we run out of
 * output buffer space.
 *
 * The framework only invokes _read() if it previously indicated we were out of
 * buffer space.  In that case, js_waiting should not generally be set, since
 * that means we last came to rest because of buffer space exhaustion rather
 * than waiting for upstream data.  Nevertheless, Node appears to do this
 * sometimes, so we defensively just do nothing in that case.  This is a little
 * scary, since incorrectness here can result in the program exiting zero
 * without having read all the data, but the surrounding program verifies that
 * this doesn't happen.
 */
JoinStream.prototype._read = function ()
{
	if (this.js_working || this.js_waiting) {
		return;
	}

	this.kick();
};

/*
 * kickIfWaiting() is invoked when an upstream source either becomes readable or
 * ended.  If we're currently waiting for upstream data, then we kick ourselves
 * to try to process whatever we've got.  Otherwise, we do nothing.
 */
JoinStream.prototype.kickIfWaiting = function ()
{
	if (!this.js_waiting) {
		return;
	}

	this.js_waiting = false;
	this.kick();
};

/*
 * kick() is invoked when we may have more data to process.  This happens in one
 * of two cases:
 *
 *     o When the framework wants the next datum from the stream.  This happens
 *       when the consumer first calls read() (since there is no data buffered)
 *       and when the consumer subsequently calls read() and the buffer is
 *       empty.  In principle, this could also happen even with some data
 *       already buffered if the framework just decides it wants to buffer more
 *       data in advance of a subsequent read.
 *
 *       The important thing in this case is that we're transitioning from being
 *       at rest because the framework wants no more data (i.e., because of
 *       backpressure) to actively reading data from our upstream sources.
 *
 *     o When we were previously blocked on an upstream source, and an upstream
 *       source now has data (or end-of-stream) available.  In this case, we're
 *       transitioning from being at rest because we were blocked on an upstream
 *       read to actively reading because at least one upstream source is now
 *       ready.
 *
 * This function decides what to do based on the current state.  If there's data
 * (or end-of-stream) available from all sources, it constructs the next output
 * and emits it.  It repeats this until either we get backpressure from the
 * framework or one of the upstream sources has no data available.
 *
 * In principle, it would be possible to invoke this at any time, but for
 * tightness, we're careful to keep track of which case we're in.  We maintain
 * the invariant that when kick() is called, js_waiting must be cleared.
 */
JoinStream.prototype.kick = function ()
{
	var i, src, datum, blocked;
	var joinkey, output, err;
	var keepgoing;

	mod_assertplus.ok(!this.js_waiting);
	mod_assertplus.ok(!this.js_working);

	/*
	 * Iterate the input streams and make sure we have the next row
	 * available from each of them.  If we don't, we'll be blocked and won't
	 * be able to make forward progress until we do.
	 */
	blocked = false;
	for (i = 0; i < this.js_sources.length; i++) {
		src = this.js_sources[i];
		if (src.src_next !== null || src.src_done) {
			continue;
		}

		datum = src.src_stream.read();
		if (datum === null) {
			blocked = blocked || !src.src_done;
			continue;
		}

		mod_assertplus.ok(Array.isArray(datum));
		src.src_next = datum;
		src.src_next_join = datum[this.js_joinidx];

		if (src.src_next_join < src.src_last_join) {
			err = new VError('source %d: out of order', i);
			this.vsWarn(err, 'out of order');
			this.emit('error', err);
			return;
		}
	}

	if (blocked) {
		this.js_waiting = true;
		return;
	}

	/*
	 * By this point, we have the next row from each source.  Figure out the
	 * earliest join key among them.
	 */
	joinkey = null;
	for (i = 0; i < this.js_sources.length; i++) {
		src = this.js_sources[i];
		if (src.src_next_join !== null &&
		    (joinkey === null || joinkey > src.src_next_join)) {
			joinkey = src.src_next_join;
		}
	}

	if (joinkey === null) {
		mod_assertplus.equal(0, this.js_sources.filter(
		    function (s) { return (!s.src_done); }).length);
		this.js_working = true;
		this.push(null);
		this.js_working = false;
		return;
	}

	/*
	 * Now, emit one array value which contains:
	 *
	 * o the join key itself
	 * o the arrays representing the corresponding values from each source,
	 *   or "null" for sources that don't contain the join key
	 */
	output = [ joinkey ];
	for (i = 0; i < this.js_sources.length; i++) {
		src = this.js_sources[i];
		if (joinkey == src.src_next_join) {
			output.push(src.src_next);
			src.src_last_join = src.src_next_join;
			src.src_next_join = null;
			src.src_next = null;
		} else {
			output.push(null);
		}
	}

	this.js_working = true;
	keepgoing = this.push(output);
	this.js_working = false;

	if (keepgoing) {
		this.kick();
	}
};

/*
 * This stream takes the output of a MergeStream and for each datum, emits a
 * string summarizing what it represents.  There are several possible cases:
 *
 *     o both counts are present, non-zero, and match up
 *     o both counts are present and don't match up
 *       (indicates an error)
 *     o only raw count present
 *       (must be non-zero, indicates a missing reported count)
 *     o only a reported count record, and it's zero
 *       (indicates a leaked count)
 *     o only a reported count record, and it's non-zero
 *       (count mismatch)
 *
 * Note that this case isn't possible:
 *
 *     o both counts are present and zero [and match up]
 *
 * because the raw count is computed from the existing entries, so it won't
 * exist if there were no entries.
 *
 * Named arguments include:
 *
 *     verbose		emit report for rows with no issues
 *
 *     [streamOptions]	passed to Transform constructor
 */
function AuditStream(args)
{
	var streamoptions;

	mod_assertplus.object(args, 'args');
	mod_assertplus.bool(args.verbose, 'args.verbose');
	mod_assertplus.optionalObject(args.streamOptions, 'args.streamOptions');

	streamoptions = mod_jsprim.mergeObjects(args.streamOptions,
	    { 'objectMode': true }, { 'highWaterMark': 0 });
	mod_stream.Transform.call(this, streamoptions);
	mod_vstream.wrapTransform(this);

	this.as_verbose = args.verbose;
}

mod_util.inherits(AuditStream, mod_stream.Transform);

AuditStream.prototype.nprocessed = function ()
{
	return (this.vsCounters()['ninputs']);
};

AuditStream.prototype._transform = function (entry, _, callback)
{
	mod_assertplus.ok(Array.isArray(entry));
	mod_assertplus.equal(entry.length, 3);

	this.emitRow(entry[0],
	    entry[1] === null ? null : entry[1][0],
	    entry[2] === null ? null : entry[2][0]);
	setImmediate(callback);
};

AuditStream.prototype.emitRow = function (dirname, rawcount, count)
{
	var message;

	message = null;
	if (count === null) {
		mod_assertplus.ok(rawcount !== null);
		if (rawcount === 0)
			message = 'warn: missing optimized count';
		else
			message = 'error: count mismatch (no optimized count)';
	}

	if (rawcount === null) {
		mod_assertplus.ok(count !== null);
		if (count === 0)
			message = 'warn: leaked optimized count';
		else
			message = 'error: count mismatch (no entries)';
	}

	if (message === null && count != rawcount) {
		message = 'error: count mismatch';
	}

	if (message === null) {
		if (!this.as_verbose)
			return;

		message = 'count okay';
	}

	this.emitRowRaw(dirname, rawcount || 0, count || 0, message);
};

AuditStream.prototype.emitRowRaw = function (dirname, rawcount, count, message)
{
	mod_assertplus.equal(typeof (dirname), 'string');
	mod_assertplus.equal(typeof (count), 'number');
	mod_assertplus.equal(typeof (rawcount), 'number');
	mod_assertplus.equal(typeof (message), 'string');

	this.push(JSON.stringify({
	    'dirname': dirname,
	    'ndirents': rawcount,
	    'nreported': count,
	    'status': message
	}) + '\n');
};

main();

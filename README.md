## Background

A number of Manta housekeeping operations are driven by daily dumps of the
Manatee shards that make up the metadata tier.  The timeline is supposed to
be:

    0000Z: Snapshots created, dump starts, then uploaded
    0200Z: Uploads done, job kicked off to unpack them
    Later: GC, metering, and audit jobs operate on unpacked dumps

For full details, see
https://github.com/joyent/manta-mola/blob/master/docs/system-crons.md

It's not uncommon (particularly lately) for the dump upload to be late,
causing the whole day's pipeline to stop without having done anything.  To
recover, operators must make sure the backups are present, then kick off
individual "backfill" jobs to unpack the dumps, then kick off whatever other
jobs need to be run.  The process of figuring out what steps need to be
completed and how to do them is currently a pretty ad-hoc, undocumented
procedure (involving "/var/tmp/fred/check.sh" in the ops zone, which isn't
shipped with the software).

In order to see this problem coming and to analyze why dump times vary, it's
also helpful to be able to plot dump times and dump sizes over time.


## Proposal

To streamline the manual steps, it would be useful to have a built-in tool to
answer these questions:

- Given a day, check for missing or late Manatee backups
- Given a day, check for missing or incomplete storage metering data
  (and suggest commands for fixing this)
- Given a day, check for missing or incomplete request metering data
  (and suggest commands for fixing this)
- Given a day, check for missing or incomplete compute metering data
  (and suggest commands for fixing this)
- Given a day, check for missing or incomplete summary metering data
  (and suggest commands for fixing this)

To help understand historical dump times, it would be helpful if the tool
facilitated:

- Given a date range, print (or plot) Manatee backup time and size


## manta-hk: managing housekeeping operations

(Better name suggestions welcome.)

    manta-hk dumps [--date DATE] [--ndays NDAYS] [--shard SHARDNAME]
        [--gnuplot]

Given a date range (default: today) and shard name (default: all shards), print
a list of all dumps found, their times, and their sizes.  With --gnuplot, emits
a combined GNUplot command and data file for plotting times and sizes.


    manta-hk metering-reports [--date DATE] [--ndays NDAYS]

Given a date range (default: last few days), print a summary of the storage,
request, compute, and summary metering data that exists for those days.

For summary and storage metering, this checks for the presence of the summary
objects and attempts to determine if entries are missing.

For compute and request metering, this checks that all expected objects have
been created.


    manta-hk verify [--date DATE]

Given a date range (default: last few days), fetches the same data provided by
the "dumps" and "metering" subcommands and attempts to detect the following
problems:

* missing Manatee dumps
* missing objects extracted from Manatee dumps (backfill needed)
* missing or incomplete metering data

If there's anything wrong, this command will report that and suggest commands to
resolve the problem.

## Future work

    manta-hk audit
    manta-hk cruft
    manta-hk gc
    manta-hk metering
    manta-hk rebalance

These subcommands fetch information about recently run housekeeping jobs.  See
https://devhub.joyent.com/jira/browse/MANTA-2593 for details.

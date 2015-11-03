# manta-hk 1 "2015" Manta "Manta internal commands"

## NAME

manta-hk - manage Manta housekeeping operations

## SYNOPSIS

manta-hk dumps [--date DATE] [--ndays NDAYS] [--shard SHARDNAME]
    [--gnuplot]

manta-hk metering-reports [--date DATE] [--ndays NDAYS]

manta-hk dircount-audit launch [--date DATE]

manta-hk dircount-audit list 

manta-hk dircount-audit report JOBID

manta-hk dircount-audit genfix JOBID

## DESCRIPTION

Manta housekeeping operations comprise a pipeline that starts with daily
database dumps of the metadata tier.  These dumps are unpacked, then processed
by various jobs to carry out metering, garbage collection, auditing, and other
basic operations.  This process is documented under "System Crons" under the
Mola subproject at https://github.com/joyent/manta-mola.  For unfortunate
historical reasons, this pipeline is currently time-based rather than
dependency-based.  As a result, when one step takes too long (e.g., dump
uploading), the pipeline either stops or (in some cases) produces incomplete
output.  This tool exists to assess the status of these operations and to check
for common causes of pipeline issues.  This tool also contains commands for
other ad-hoc housekeeping operations that are not normally required.

**The options and output of this command are not committed.  This command should
not be used programmatically.**

The "dumps" subcommand examines the daily dumps of the metadata tier for a given
date range and summarizes which dumps are missing, arrived late, or have not
been unpacked.  Dumps that are late or missing are marked with an asterisk
("\*").  Dumps that have not been unpacked are marked with an exclamation point
("!").  The --gnuplot option is useful for plotting database dump time and size
to help understand long-term trends.

The "metering-reports" subcommand examines the metering reports produced
regularly by Manta and reports for a given date range which reports are missing
or may be incomplete.

The "dircount-audit" subcommands allows operators to launch, list, and summarize
the results of jobs used to audit directory counts and to generate commands to
repair these counts.  As a result of previous software defects, directory counts
in the metadata tier could become incorrect.  The "dircount-audit launch"
subcommand locates database backups for the given day and launches jobs on these
objects that compare the actual and reported directory counts.  The
"dircount-audit list" subcommand lists running or recently-run jobs started with
"dircount-audit launch".  The "dircount-audit report" subcommand summarizes the
results.  The "dircount-audit genfix" subcommand generates SQL commands to
repair incorrect directory counts.  For more, see "Directory count audit jobs"
below.

All invocations of this tool use the public interface to Manta to examine data
stored in Manta itself.  This data is available only to operator accounts.  Some
invocations of this tool also manage jobs.  The `MANTA_URL`, `MANTA_USER`, and
`MANTA_KEY_ID` environment variables must be set as you would set them to use
the Manta command-line tools (e.g., mls(1)) as an operator.


## Directory count audit jobs

The "dircount-audit report" command reports the number of directories with
incorrect size counts.  For each directory, there are five possible states:

"count okay"

    The reported (saved) directory count matches the number of entries in
    the directory.  This is only shown when the job was run in verbose
    mode.

"warn: leaked optimized count"

    There are no entries in the directory (possibly because it does not
    exist), and there's a reported (saved) directory count of 0.  While the
    reported count matches the actual count, there are not supposed to be
    any reported counts of zero.  Counts are supposed to be removed when
    they reach zero.  Thus, the counter for this entry is leaked.

"error: count mismatch (no optimized count)"

    There are entries in the directory, but the reported count is zero
    because there is no saved count.

"error: count mismatch (no entries)"

    There are no entries in the directory, but there's a non-zero reported
    count.

"error: count mismatch"

    There are entries in the directory, and there is a saved count for
    the directory, but the two counts do not match.


## OPTIONS

`-d, --date DATE`
  For "dumps" and "metering-reports", specifies the end of the date range to
  examine.  For "dircount-audit launch", specifies which day's database dumps to
  audit.  DATE should be an ISO 8601 timestamp that includes at least the full
  date part (e.g., "2015-07-13T00:00:00Z" or just "2015-07-13").  For the
  "dumps" command, the time part will be ignored.  With `-D, --days NDAYS`,
  manta-hk will examine information for the NDAYS preceding DATE (including
  DATE) itself.  The default is the most recent reasonable date.  For "dumps"
  and "metering-reports", that's the current date.  For "dircount-audit launch",
  that's today if the dumps are expected to be present already, or else
  yesterday.

`-D, --days NDAYS`
  Specifies how many days before DATE should be examined.  The default is a few
  days (which is intentionally vague, as this is subject to change).

`-s, --shard SHARDNAME`
  ("dumps" subcommand only) Specifies that only database dumps for shard
  SHARDNAME should be examined.  By default, all shards' dumps are examined.
  You can specify this option multiple times to examine multiple shards.

`--gnuplot`
  ("dumps" subcommand only) With this flag, the "dumps" command emits to stdout
  a combined command and data file for use with gnuplot(1) to print a graph of
  database dump size and completion time over the specified period.  See
  EXAMPLES below.


## ENVIRONMENT

`MANTA_URL`, `MANTA_USER`, `MANTA_KEY_ID`
  See mls(1).


## EXAMPLES

List information about dumps for the last few days:

    $ manta-hk dumps
    Dumps for 2015-07-11      SHARD     MB  ELAPSED          ENDED  #UNPACKED
           1.moray.emy-10.joyent.us     79    0m32s  00:00:31.222Z  10
           2.moray.emy-10.joyent.us    175    5m05s  00:05:04.694Z  5
           3.moray.emy-10.joyent.us    352    5m07s  00:05:06.916Z  5
    Dumps for 2015-07-12      SHARD     MB  ELAPSED          ENDED  #UNPACKED
           1.moray.emy-10.joyent.us     72    0m30s  00:00:29.739Z  10
           2.moray.emy-10.joyent.us    248    6m37s  00:06:36.660Z  5
           3.moray.emy-10.joyent.us    175    4m21s  00:04:20.965Z  5
    Dumps for 2015-07-13      SHARD     MB  ELAPSED          ENDED  #UNPACKED
           1.moray.emy-10.joyent.us     77    0m40s  00:00:39.958Z  10
           2.moray.emy-10.joyent.us    153    5m46s  00:05:45.085Z  5
           3.moray.emy-10.joyent.us    251    4m52s  00:04:53.959Z  5
    $

List information about dumps from shards 3 and 4 on July 3, 2015:

    $ manta-hk dumps --date=2015-07-03 --days=1 
        --shard=3.moray.us-east.joyent.us --shard=4.moray.us-east.joyent.us
    Dumps for 2015-07-03      SHARD     MB  ELAPSED          ENDED  #UNPACKED
          3.moray.us-east.joyent.us   6897   15m28s  00:15:27.342Z  5
          4.moray.us-east.joyent.us   6033   11m14s  00:11:13.595Z  5
    $

When a problem is causing dumps to take too long to upload, the output may look
instead like this:

    Dumps for 2015-07-12      SHARD     MB  ELAPSED          ENDED  #UNPACKED
           1.moray.emy-10.joyent.us     72    0m30s  00:00:29.739Z  10
    *      2.moray.emy-10.joyent.us    237  125m32s  02:05:31.578Z  5
           3.moray.emy-10.joyent.us    205    6m37s  00:06:36.660Z  5
           4.moray.emy-10.joyent.us    383    4m21s  00:04:20.965Z  5

When a problem is preventing the dumps from being unpacked, the output may look
instead like this:

    Dumps for 2015-07-13     SHARD     MB  ELAPSED          ENDED  #UNPACKED
          1.moray.emy-10.joyent.us     68    0m56s  00:00:55.160Z  14
    !     2.moray.emy-10.joyent.us     49    0m49s  00:00:48.379Z  0

Plot dump time for all shards for the last 180 days, storing the result in
"graph.png" (this may take a few minutes):

    $ manta-hk dumps --days=180 --gnuplot | gnuplot > graph.png
    $

Show information about metering reports from the last few days:

    $ manta-hk metering-reports
    Metering reports for 2015-07-09
         summary report:  517 entries
         storage report:  504 entries
        compute reports:  24/24 present
        request reports:  24/24 present
    Metering reports for 2015-07-10
         summary report:  527 entries
         storage report:  517 entries
        compute reports:  24/24 present
        request reports:  24/24 present
    Metering reports for 2015-07-11
         summary report:  533 entries
         storage report:  527 entries
        compute reports:  24/24 present
        request reports:  24/24 present
    $

When run in the middle of the day, the summary report and some of the later
hours may be missing, which may look like this:

    Metering reports for 2015-07-13
         summary report:  MISSING
         storage report:  6537 entries
        compute reports:  18/24 present (hours missing: 18, 19, 20, 21 and 2 more)
        request reports:  18/24 present (hours missing: 18, 19, 20, 21 and 2 more)


## SEE ALSO

This repository is part of the Joyent Manta project.  For contribution
guidelines, issues, and general documentation, visit the 
Manta project page at http://github.com/joyent/manta.

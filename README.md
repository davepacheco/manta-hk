# manta-hk

This repository is part of the Joyent Manta project.  For contribution
guidelines, issues, and general documentation, visit the main
[Manta](http://github.com/joyent/manta) project page.

manta-hk is a tool for managing Manta housekeeping operations.

For background and usage information, see
[docs/man/manta-hk.md](docs/man/manta-hk.md).

## Future work

    manta-hk verify [--date DATE]

Given a date range (default: last few days), fetches the same data provided by
the "dumps" and "metering" subcommands and attempts to detect the following
problems:

* missing Manatee dumps
* missing objects extracted from Manatee dumps (backfill needed)
* missing or incomplete metering data

If there's anything wrong, this command will report that and suggest commands to
resolve the problem.

    manta-hk audit
    manta-hk cruft
    manta-hk gc
    manta-hk metering
    manta-hk rebalance

These subcommands fetch information about recently run housekeeping jobs.  See
https://devhub.joyent.com/jira/browse/MANTA-2593 for details.

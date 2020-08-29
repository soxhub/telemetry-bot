# Version Migrations

This directory contains sql scripts for schema setup and upgrading.

All scripts are contained in the `sql` directory and are separated as follows:

1. `preinstall` - This directory contains all scripts that will be executed on
    a new database install.
2. `idempotent` - This directory contains all scripts that contain idempotent
    content which is executed after a fresh install or a version upgrade.
3. `versions/dev` - This directory contains subdirectories that are named after
    the timescale-prometheus version they were introduced in.

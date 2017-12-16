
# ec2 access keys
access_id = "<the_id>"
access_secret = "<the_secret>"

# enable debug logging
debug_logging = True

# enable console output (in addition to syslog)
console_output = True

# do everything but the actual backup calls
dry_run = True

# the region to be backed up
primary_region = "us-west-2"

# the backup region to copy amis to
backup_region = "us-west-1"

# by default the script will backup all running instances
# the include and exclude give some fine-tuning to this

# list of regexes of server names to include
# included servers are always backed up, running or not
# example list:
# include_instances = (
#     r'na-whatever-not-you',
#     r'na-whatever-automated-*',
# )
include_instances = ()

# list of regexes of server names to exclude
# excluded servers are never backed up, running or not
exclude_instances = (
    r'aws-oregon-apache1',
)

# if an instance isn't specifically included or excluded
# then this will backup any instance that's running
backup_running_instances = True

# wait a f-ing long time for the ami's to create.  3 hours
image_wait_timeout = 60 * 60 * 3
# the long wait time is for a first copy of a disk image to a
# different region.  after the first copy it does differentials

# set keep_n_weeks or keep_n_months to 0 to disable weekly
# and/or monthly backups

# keep 7 days of daily
keep_n_days = 7
# 2 months (8 weeks) of weekly
keep_n_weeks = 8
# and 4 months of monthly
keep_n_months = 4

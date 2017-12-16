# from pprint import pprint

import boto.ec2
from boto.ec2.regioninfo import RegionInfo
from boto.exception import EC2ResponseError
import syslog
import datetime
import time
import calendar
import re
import math
import ec2_backup_config
import sys
from pprint import pprint
import base64
import subprocess

script_version = 'v6-02062017'

completed_backups = 0
total_instance_time = 0
error_count = 0
failed_copies = 0
count_str = ''

backup_start_time = time.time()


def log(message, priority="notice"):
    """send logs to syslog"""
    print_date = "{:%c}".format(datetime.datetime.now())
    if priority == 'error':
        syslog.syslog(syslog.LOG_ERR, 'error: ' + message)
        if ec2_backup_config.console_output: print print_date, priority + ': ', message
    elif priority == 'debug':
        if ec2_backup_config.debug_logging:
            syslog.syslog(syslog.LOG_DEBUG, 'debug: ' + message)
            if ec2_backup_config.console_output: print print_date, priority + ': ', message
    else:
        syslog.syslog(syslog.LOG_NOTICE, 'notice: ' + message)
        if ec2_backup_config.console_output: print print_date, priority + ':', message
    return


def secs_to_str(time):
    """takes a seconds float and returns a nicely formatted string"""
    time_list = []
    if time > 3600:
        hours = int(math.floor(time / 3600))
        time = time - (hours * 3600)
        if hours == 1:
            hours_str = '1 hr'
        else:
            hours_str = str(hours) + ' hrs'
        time_list.append(hours_str)
    if time > 60:
        minutes = int(math.floor(time / 60))
        if minutes == 1:
            minutes_str = '1 min'
        else:
            minutes_str = str(minutes) + ' mins'
        time = time - (minutes * 60)
        time_list.append(minutes_str)
    time = int(time)
    if time >= 0:
        if time == 1:
            time_list.append('1 sec')
        else:
            time_list.append(str(time) + ' secs')
    time_list = ', '.join(time_list)
    return time_list


def error_report(error_text, fatal=False):
    """logs and calls home to zabbix for an alert"""
    global error_count
    error_count += 1
    log('error #' + str(error_count) + ': ' + str(error_text), 'error')
    if fatal:
        log('fatal error, exiting', 'error')
        exit(1)
    if error_count > 4:
        error_report('too many errors on this run (' + str(error_count) + '), exiting', True)


def instance_backup(instance_name='', i=None, debug=False, logging=True):
    """takes instance name and the instance and instance object, returns if it should be backed up"""
    global ec2_backup_config, count_str
    # check the include list to see if it's always backed up
    for j in ec2_backup_config.include_instances:
        try:
            match = re.search(j, instance_name)
        except:
            if debug and logging:
                print 'problem with include_instances regex "' + str(j) + '"'
            else:
                error_report(
                    count_str + ' problem with include_instances regex "' + str(j) + '": ' + str(sys.exc_info()[0]))
            match = False
        if match:
            if debug and logging:
                print instance_name + ' will be included with regex "' + str(j) + '"'
            elif logging:
                log(count_str + ' instance ' + instance_name + ' will be included by regex', 'notice')
            return True

    # check the exclude list to see if it's always skipped
    for j in ec2_backup_config.exclude_instances:
        try:
            match = re.search(j, instance_name)
        except:
            if debug and logging:
                print 'problem with exclude_instances regex "' + str(j) + '"'
            else:
                error_report(
                    count_str + ' problem with exclude_instances regex "' + str(j) + '": ' + str(sys.exc_info()[0]))
            match = False
        if match:
            if debug and logging:
                print instance_name + ' will be excluded with regex "' + str(j) + '"'
            elif logging:
                log(count_str + ' instance ' + instance_name + ' will be excluded by regex', 'notice')
            return False

    # see if it's running and back it up if so
    if ec2_backup_config.backup_running_instances:
        try:
            if str(i.state) == 'running':
                if debug and logging:
                    print instance_name + ' is running and will be included'
                elif logging:
                    log(count_str + ' instance ' + instance_name + ' is running and will be included', 'notice')
                return True
            else:
                if debug and logging:
                    print instance_name + ' is ' + str(i.state) + ' and will be excluded'
                elif logging:
                    log(count_str + ' instance ' + instance_name + ' is ' + str(i.state) + ' and will be excluded',
                        'notice')
                return False
        except:
            if debug and logging:
                print instance_name + ' will be included if it is running'
            elif logging:
                log('cannot get state for ' + instance_name + ', excluding', 'notice')
            return False

    if debug and logging:
        print instance_name + ' will be excluded by default'
    elif logging:
        log(count_str + ' instance ' + instance_name + ' will be excluded by default', 'notice')
    return False


test_all_hosts = False
if len(sys.argv) > 1:
    if sys.argv[1] == 'test-name':
        try:
            test_host = sys.argv[2]
        except:
            print 'usage: test-name [hostname]'
            exit(0)
        # this will output the result with debug set to true
        instance_backup(instance_name=test_host, i=None, debug=True, logging=True)
        exit(0)
    elif sys.argv[1] == 'test-all-hosts':
        test_all_hosts = True
    else:
        print 'options:'
        print 'test-name [hostname] : test to see how a host name is handled'
        print 'test-all-hosts : tests all hostnames to see how they are handled'
        exit(0)


class Queue:
    """Queue and slots for the other classes to use"""

    def __init__(self, num_slots=1):
        # items stores the list of things to do
        self.items = []
        # slots represent concurrent jobs
        self.slots = []
        # num_slots is the number of concurrent jobs
        self.num_slots = num_slots

    def enqueue(self, item):
        # add an instance to the queue
        item.status = 'new'
        self.items.insert(0, item)
        self.fill_slots()

    def dequeue(self):
        # remove next item from the queue
        return self.items.pop()

    def size(self):
        # return length of the queue
        return len(self.items)

    def get_slots(self):
        # return a copy of the slots list
        self.fill_slots()
        return list(self.slots)

    def get_slots_size(self):
        # return length of the filled slots
        return len(self.slots)

    def clear_slot(self, slot):
        # remove an item from the slots list
        for instance in self.get_slots():
            if instance.id == slot.id:
                self.slots.remove(instance)

    def get_new_slot(self):
        # returns an instance and fills the slot
        if len(self.slots) < self.num_slots and self.size() > 0:
            new_item = self.dequeue()
            self.slots.append(new_item)
            return new_item
        else:
            return None

    def fill_slots(self):
        # fill up any available slots with queued items
        while len(self.slots) < self.num_slots:
            if self.size() == 0:
                break
            self.slots.append(self.dequeue())

    def total_queue_size(self):
        return self.size() + self.get_slots_size()


class Instance:
    """An instance"""

    def __init__(self, i):
        # i is the instance as returned by boto
        self.i = i
        # name is the calculated name for backups (64 is for base 64 version of it)
        self.name = ''
        self.name64 = ''
        # name and id as one var (for logs)
        self.name_id = ''
        # image name and description for amis
        self.image_name = ''
        self.image_desc = ''
        self.image_name64 = ''
        self.image_desc64 = ''
        # region
        self.region = ''
        # id is the instance id
        self.id = i.id
        # the count string for this instance (used for logging)
        self.count_str = ''
        # the status of the instance (new, waiting, etc)
        self.status = 'new'
        # total time for the backup (queue waiting, create, copy, purge)
        self.total_time = 0
        # start and end times for complete backup (set to time.time())
        self.bu_start_time = None
        self.bu_end_time = None
        # used by the creator to determine if there are pre-existing backups
        self.existing_images = []
        # backup type - daily,weekly,monthly
        self.backup_type = ''
        # image start time (about 99.9% of the backup time)
        self.image_start_time = None
        # ami image id for new image
        self.ami_image_id = None
        # ami object
        self.ami_object = None
        # copy ami object
        self.copy_object = None
        # today stamp for future purges
        self.today_stamp = ''
        # previous ami id for the copier to use as source
        self.previous_ami_id = ''

    def start_timer(self):
        self.bu_start_time = time.time()

    def stop_timer(self):
        self.bu_end_time = time.time()
        self.total_time += self.bu_end_time - self.bu_start_time

    def bu_time(self):
        return secs_to_str(self.bu_end_time - self.bu_start_time)

    def update_status(self, t):
        self.status = t


class ImagePurge:
    """Purges one ami for primary and backup regions"""

    def __init__(self):
        self.queue = Queue(3)
        self.purge_count = 0
        self.boto_conns = []
        self.type = 'purge'

        # calculate the oldest backup times for each bucket
        self.oldest_daily = datetime.date.today() - datetime.timedelta(ec2_backup_config.keep_n_days)
        self.oldest_weekly = datetime.date.today() - datetime.timedelta(ec2_backup_config.keep_n_weeks * 7)
        self.oldest_monthly = datetime.date.today() - datetime.timedelta(ec2_backup_config.keep_n_months * (365 / 12))

        log('oldest daily backup: ' + self.oldest_daily.isoformat() + ' (' +
            str(ec2_backup_config.keep_n_days) + ' days)', 'debug')
        if ec2_backup_config.keep_n_weeks > 0:
            log('oldest weekly backup: ' + self.oldest_weekly.isoformat() + ' (' +
                str(ec2_backup_config.keep_n_weeks) + ' weeks)', 'debug')
        if ec2_backup_config.keep_n_months > 0:
            log('oldest monthly backup: ' + self.oldest_monthly.isoformat() + ' (' +
                str(ec2_backup_config.keep_n_months) + ' months)', 'debug')

        # convert the times into timestamps we can compare with later
        self.oldest_daily = calendar.timegm(self.oldest_daily.timetuple())
        self.oldest_weekly = calendar.timegm(self.oldest_weekly.timetuple())
        self.oldest_monthly = calendar.timegm(self.oldest_monthly.timetuple())

    def check_slots(self):
        global total_instance_time, completed_backups
        self.queue.fill_slots()
        for instance in self.queue.get_slots():
            instance.start_timer()
            self.run_purge(instance)
            instance.stop_timer()
            log(instance.count_str + ' ' + self.type + ': completed purge for ' + instance.name_id + ' in ' +
                instance.bu_time(), 'debug')
            total_backup_time = secs_to_str(instance.total_time)
            total_instance_time += instance.total_time
            completed_backups += 1
            log(instance.count_str + ' ' + self.type + ': completed backup for ' + instance.name_id + ' in ' +
                total_backup_time, 'notice')
            self.queue.clear_slot(instance)
            time.sleep(0.5)

    def run_purge(self, instance):
        for boto_conn in self.boto_conns:
            # next to last thing left is to purge any old images and snapshots in primary region
            try:
                image_list = boto_conn.get_all_images(owners=['self'], filters={
                    "tag:" + ec2_backup_config.tag_hostname: instance.name})
            except EC2ResponseError as e:
                error_report(instance.count_str + ' ' + self.type + ': problem getting primary purge list for ' +
                             instance.name_id + ': ' + str(e.error_code))
                image_list = []
            except:
                error_report(instance.count_str + ' ' + self.type + ': problem getting primary purge list for ' +
                             instance.name_id + ': ' + str(sys.exc_info()[0]))
                image_list = []

            if len(image_list) > 0:
                log(instance.count_str + ' ' + self.type + ': found ' + str(len(image_list)) + ' existing images for ' +
                    instance.name_id, 'debug')
            for j in image_list:
                try:
                    bu_img_type = j.tags[ec2_backup_config.tag_backup_type]
                except:
                    log(instance.count_str + ' ' + self.type + ': backup type tag (' +
                        ec2_backup_config.tag_backup_type + ') missing in ' + j.id + ' for ' + instance.name_id,
                        'error')
                    continue
                try:
                    bu_img_stamp = int(j.tags[ec2_backup_config.tag_timestamp])
                except:
                    log(instance.count_str + ' ' + self.type + ': backup timestamp tag (' +
                        ec2_backup_config.tag_timestamp + ') missing in ' + j.id + ' for ' + instance.name_id, 'error')
                    continue

                # find the latest possible purge stamp based on the backup type
                # every backup is a daily so we can start there
                bu_purge_stamp = self.oldest_daily
                if ec2_backup_config.keep_n_weeks > 0 and 'Weekly' in bu_img_type:
                    if self.oldest_weekly < bu_purge_stamp:
                        bu_purge_stamp = self.oldest_weekly
                if ec2_backup_config.keep_n_months > 0 and 'Monthly' in bu_img_type:
                    if self.oldest_monthly < bu_purge_stamp:
                        bu_purge_stamp = self.oldest_monthly
                # compare the image timestamp to the purge timestamp
                if bu_img_stamp < bu_purge_stamp:
                    log(instance.count_str + ' ' + self.type + ': purging old backup: ' + j.name + ' (' + j.id + ')',
                        'notice')
                    if not ec2_backup_config.dry_run:
                        # make a list of the snapshot ids associated to it
                        block_device_list = []
                        for key, block_device in j.block_device_mapping.iteritems():
                            block_device_list.append(block_device.snapshot_id)
                        # deregister the ami
                        log(instance.count_str + ' ' + self.type + ': deregistering ' + j.id + ' for ' +
                            instance.name_id, 'debug')
                        try:
                            boto_conn.deregister_image(j.id)
                        except EC2ResponseError as e:
                            error_report(instance.count_str + ' ' + self.type + ': problem deregistering ' + j.id +
                                         ' for ' + instance.name_id + ': ' + str(e.error_code))
                        except:
                            error_report(instance.count_str + ' ' + self.type + ': problem deregistering ' + j.id +
                                         ' for ' + instance.name_id + ': ' + str(sys.exc_info()[0]))
                        # delete the snapshots associated to it
                        for snap_id in block_device_list:
                            log(instance.count_str + ' ' + self.type + ': deleting snapshot ' + str(snap_id) +
                                ' under ' + j.id + ' for ' + instance.name_id, 'debug')
                            try:
                                if snap_id is not None:
                                    boto_conn.delete_snapshot(snap_id)
                            except EC2ResponseError as e:
                                log(instance.count_str + ' ' + self.type + ': problem deleting snapshot ' +
                                    str(snap_id) + ' under ' + j.id + ' for ' + instance.name_id + ': ' +
                                    str(e.error_code), 'notice')
                            except:
                                log(instance.count_str + ' ' + self.type + ': problem deleting snapshot ' +
                                    str(snap_id) + ' under ' + j.id + ' for ' + instance.name_id + ': ' +
                                    str(sys.exc_info()[0]), 'notice')


class Copier:
    """Base class for the image makers"""

    def __init__(self):
        self.queue = Queue(3)
        self.backup_count = 0
        self.boto_conn = None
        # type - primary or backup
        self.type = '<not set>'
        # run this backup or skip it
        self.active = True

    def start_backup(self, instance):
        self.backup_count += 1
        log(instance.count_str + ' ' + self.type + ': processing ' + instance.name_id, 'notice')
        log(instance.count_str + ' ' + self.type + ': checking for existing backup', 'debug')
        if self.has_existing_bu(instance):
            return False
        instance.start_timer()
        if self.create_image(instance):
            instance.update_status('waiting')
            return True
        else:
            return False

    def start_copy(self, instance):
        self.backup_count += 1
        log(instance.count_str + ' ' + self.type + ': processing ' + instance.name_id, 'notice')
        log(instance.count_str + ' ' + self.type + ': checking for existing backup', 'debug')
        if self.has_existing_bu(instance):
            return False
        instance.start_timer()
        if self.create_copy(instance):
            instance.update_status('waiting')
            return True
        else:
            return False

    def stop_backup(self, instance):
        # figure out how long it took and log it
        instance.stop_timer()
        log(instance.count_str + ' ' + self.type + ': completed ' + instance.ami_image_id +
            ' for ' + instance.name_id + ' in ' + instance.bu_time(), 'notice')

    def is_copy_done(self, instance):
        global failed_copies
        # check to see if the backup timed out
        current_time = time.time()
        elapsed_time = int(current_time - instance.image_start_time)
        if elapsed_time > ec2_backup_config.image_wait_timeout:
            error_report(
                instance.count_str + ' ' + self.type + ': timed out at ' + secs_to_str(elapsed_time) +
                ' creating ' + instance.ami_image_id + ' for ' + instance.name_id + ', aborting this backup')
            return 'timed_out'
        # check to see if the backup is done
        try:
            img = self.boto_conn.get_image(instance.ami_image_id)
            if img.state == 'available':
                log(instance.count_str + ' ' + self.type + ': ' + instance.ami_image_id + ' for ' + instance.name_id +
                    ' is available after ' + secs_to_str(elapsed_time), 'notice')
                instance.ami_object = img
                return 'done'
            elif img.state == 'failed':
                log(instance.count_str + ' ' + self.type + ': ' + instance.ami_image_id + ' for ' + instance.name_id +
                    ' has FAILED after ' + secs_to_str(elapsed_time), 'error')
                failed_copies += 1
                return 'failed'
            elif img.state == 'pending':
                return 'no'
            else:
                log(instance.count_str + ' ' + self.type + ': unknown image status for ' + instance.ami_image_id +
                    ' for ' + instance.name_id + ': ' + str(img.state), 'notice')
                return 'no'

        except EC2ResponseError as e:
            if e.error_code == 'InvalidAMIID.NotFound':
                log(instance.count_str + ' ' + self.type + ': ' + instance.ami_image_id + ' for ' + instance.name_id +
                    ' not found yet (' + secs_to_str(elapsed_time) + ' elapsed)', 'debug')
            else:
                log(instance.count_str + ' ' + self.type + ': error checking for image copy: ' + e.error_code, 'notice')
        return 'no'

    def remove_ami(self, instance):
        # for failed ami creation we need to remove the failed ami
        j = self.boto_conn.get_image(instance.ami_image_id)
        block_device_list = []
        for key, block_device in j.block_device_mapping.iteritems():
            block_device_list.append(block_device.snapshot_id)
        # deregister the ami
        log(instance.count_str + ' ' + self.type + ': deregistering ' + j.id + ' for ' +
            instance.name_id, 'debug')
        try:
            self.boto_conn.deregister_image(j.id)
        except EC2ResponseError as e:
            error_report(instance.count_str + ' ' + self.type + ': problem deregistering ' + j.id +
                         ' for ' + instance.name_id + ': ' + str(e.error_code))
        except:
            error_report(instance.count_str + ' ' + self.type + ': problem deregistering ' + j.id +
                         ' for ' + instance.name_id + ': ' + str(sys.exc_info()[0]))
        # delete the snapshots associated to it
        for snap_id in block_device_list:
            log(instance.count_str + ' ' + self.type + ': deleting snapshot ' + str(snap_id) +
                ' for ' + instance.name_id, 'debug')
            try:
                if snap_id is not None:
                    self.boto_conn.delete_snapshot(snap_id)
            except EC2ResponseError as e:
                log(instance.count_str + ' ' + self.type + ': problem deleting snapshot ' +
                    str(snap_id) + ' for ' + j.id + ' for ' + instance.name_id + ': ' + str(e.error_code), 'notice')
            except:
                log(instance.count_str + ' ' + self.type + ': problem deleting snapshot ' +
                    str(snap_id) + ' for ' + j.id + ' for ' + instance.name_id + ': ' + str(sys.exc_info()[0]),
                    'notice')

    def tag_new_image(self, instance):
        # ami was successful so now we can tag the image
        log(instance.count_str + ' ' + self.type + ': adding tags to image for ' + instance.name_id, 'debug')
        if not ec2_backup_config.dry_run:
            try:
                instance.ami_object.add_tag(ec2_backup_config.tag_hostname, instance.name)
                instance.ami_object.add_tag(ec2_backup_config.tag_instance_id, instance.id)
                instance.ami_object.add_tag(ec2_backup_config.tag_backup_type, instance.backup_type)
                instance.ami_object.add_tag(ec2_backup_config.tag_timestamp, instance.today_stamp)
                instance.ami_object.add_tag(ec2_backup_config.tag_region, instance.region)
            except EC2ResponseError as e:
                error_report(instance.count_str + ' ' + self.type + ': problem adding tags to ' + self.type + ' ' +
                             str(instance.ami_object.id) + ' for ' + instance.name_id + ', aborting this backup: ' +
                             str(e.error_code))
                return False
            except:
                error_report(instance.count_str + ' ' + self.type + ': problem adding tags to ' + self.type + ' ' +
                             str(instance.ami_object.id) + ' for ' + instance.name_id + ', aborting this backup: ' +
                             str(sys.exc_info()[0]))
                return False
        return True

    def has_existing_bu(self, instance):
        # check to see if there are pre-existing backups
        try:
            image_list = self.boto_conn.get_all_images(owners=['self'],
                                                       filters={
                                                           "tag:" + ec2_backup_config.tag_hostname: instance.name})
        except EC2ResponseError as e:
            error_report(
                instance.count_str + ' ' + self.type + ': problem checking for pre-existing backups for ' +
                instance.name_id + ': ' + str(e.error_code))
            image_list = [1]
        except:
            error_report(
                instance.count_str + ' ' + self.type + ': problem checking for pre-existing backups for ' +
                instance.name_id + ': ' + str(sys.exc_info()[0]))
            image_list = [1]
        if len(image_list) == 0:
            log(instance.count_str + ' ' + self.type + ': first backup of ' + instance.name_id +
                ', marking this one for daily, monthly, and weekly', 'notice')
            instance.backup_type = 'Daily,Weekly,Monthly'
            return False
        else:
            instance.backup_type = self.default_backup_type
            # check to see if the image already exists
            try:
                existing_images = self.boto_conn.get_all_images(owners=['self'],
                                                                filters={"name": instance.image_name})
            except EC2ResponseError as e:
                error_report(
                    instance.count_str + ' ' + self.type + ': problem checking to see if backup exists for ' +
                    instance.name_id + ': ' + str(e.error_code))
                existing_images = []
            except:
                error_report(
                    instance.count_str + ' ' + self.type + ': problem checking to see if backup exists for ' +
                    instance.name_id + ': ' + str(sys.exc_info()[0]))
                existing_images = []
            if len(existing_images) > 0:
                log(instance.count_str + ' ' + self.type + ': backup for ' + instance.name_id +
                    ' already exists!  skipping', 'notice')
                instance.ami_object = existing_images[0]
                instance.previous_ami_id = instance.ami_object.id
                return True
            else:
                return False

    def create_image(self, instance):
        # image params
        params = {
            'instance_id': instance.id,
            'name': instance.image_name,
            'description': instance.image_desc,
            'no_reboot': True,
            'block_device_mapping': instance.i.block_device_mapping
        }
        # create the image
        if not ec2_backup_config.dry_run:
            try:
                instance.ami_image_id = self.boto_conn.create_image(**params)
            except EC2ResponseError as e:
                error_report(instance.count_str + ' ' + self.type + ': problem creating image for ' + instance.name_id +
                             ', aborting this backup: ' + str(e.error_code))
                return False
            except:
                error_report(instance.count_str + ' ' + self.type + ': problem creating image for ' + instance.name_id +
                             ', aborting this backup: ' + str(sys.exc_info()[0]))
                return False
            log(instance.count_str + ' ' + self.type + ': creating ' + str(instance.ami_image_id) + ' for ' +
                instance.name_id, 'notice')
            try:
                instance.ami_object = self.boto_conn.get_image(instance.ami_image_id)
            except:
                instance.ami_object = None
            instance.previous_ami_id = instance.ami_object.id
            instance.image_start_time = time.time()
        return True

    def create_copy(self, instance):
        # copy it to the backup region
        params = {
            'source_region': instance.region,
            'source_image_id': instance.previous_ami_id,
            'name': instance.image_name,
            'description': instance.image_desc
        }
        if not ec2_backup_config.dry_run:
            try:
                instance.ami_object = self.boto_conn.copy_image(**params)
            except EC2ResponseError as e:
                error_report(instance.count_str + ' ' + self.type + ': problem creating image for ' + instance.name_id +
                             ', aborting this backup: ' + str(e.error_code))
                return False
            except:
                error_report(instance.count_str + ' ' + self.type + ': problem creating image for ' + instance.name_id +
                             ', aborting this backup: ' + str(sys.exc_info()[0]))
                return False

            log(instance.count_str + ' ' + self.type + ': image copy ' + str(instance.ami_image_id) +
                ' is being created for ' + instance.name_id, 'notice')
            instance.ami_image_id = instance.ami_object.image_id
            instance.image_start_time = time.time()
        return True


class ImageCreator(Copier):
    """Creates up to 5 ami images concurrently, then queues it for a copy"""

    def __init__(self, imagecopier_obj, imagepurge_obj):
        self.backup_count = 0
        self.boto_conn = None
        # run this backup or skip it
        self.active = True
        self.queue = Queue(5)
        self.image_copier = imagecopier_obj
        self.image_purger = imagepurge_obj
        self.type = 'primary'

        # figure out if this is a daily, weekly, and/or monthly
        # every run is a daily backup
        self.default_backup_type = ['Daily']
        if datetime.date.today().weekday() == 6:
            # sunday, so a weekly backup
            self.default_backup_type.append('Weekly')
        if datetime.date.today().day == 1:
            # first of month, so a monthly backup
            self.default_backup_type.append('Monthly')
        self.default_backup_type = ','.join(self.default_backup_type)
        log('backup type: ' + self.default_backup_type, 'debug')

    def check_slots(self):
        self.queue.fill_slots()
        for instance in self.queue.get_slots():
            needs_new_instance = False
            if not self.active:
                log(instance.count_str + ' ' + self.type + ': queueing ' + instance.name_id + ' for copy', 'debug')
                self.image_copier.queue.enqueue(instance)
                self.queue.clear_slot(instance)
            if instance.status == 'new':
                if not self.start_backup(instance):
                    instance.stop_timer()
                    log(instance.count_str + ' ' + self.type + ': queueing ' + instance.name_id + ' for copy', 'debug')
                    self.image_copier.queue.enqueue(instance)
                    self.queue.clear_slot(instance)
                    needs_new_instance = True
                time.sleep(0.5)
            elif instance.status == 'waiting':
                copy_status = self.is_copy_done(instance)
                if copy_status == 'done':
                    if self.tag_new_image(instance):
                        log(instance.count_str + ' ' + self.type + ': queueing ' + instance.name_id + ' for copy',
                            'debug')
                        self.image_copier.queue.enqueue(instance)
                    else:
                        # goofed up tagging?  something's not right
                        log(instance.count_str + ' ' + self.type + ': queueing ' + instance.name_id + ' for purge',
                            'debug')
                        self.image_purger.queue.enqueue(instance)
                    self.stop_backup(instance)
                    self.queue.clear_slot(instance)
                    needs_new_instance = True
                elif copy_status == 'timed_out':
                    # nothing to copy if it's not done... goes straight to the purger
                    log(instance.count_str + ' ' + self.type + ': queueing ' + instance.name_id + ' for purge', 'debug')
                    self.image_purger.queue.enqueue(instance)
                    self.stop_backup(instance)
                    self.queue.clear_slot(instance)
                    needs_new_instance = True
                elif copy_status == 'failed':
                    # failed, so we'll remove the bad ami and try again
                    log(instance.count_str + ' ' + self.type + ': removing failed ami for ' + instance.name_id, 'debug')
                    self.remove_ami(instance)
                    instance.stop_timer()
                    log(instance.count_str + ' ' + self.type + ': restarting creation for ' + instance.name_id, 'debug')
                    if not self.start_copy(instance):
                        instance.stop_timer()
                        log(instance.count_str + ' ' + self.type + ': queueing ' + instance.name_id + ' for copy',
                            'debug')
                        self.image_copier.queue.enqueue(instance)
                        self.queue.clear_slot(instance)
                        needs_new_instance = True
                time.sleep(0.5)
            if needs_new_instance:
                # normally it would be a very bad idea to mess around with the slot list while we're iterating it.
                # however, the get_slots() method returns a copy of the slots list.  so we can mess with the real
                # one as much as we want without screwing up the for loop that's going.
                while 1 == 1:
                    new_instance = self.queue.get_new_slot()
                    if new_instance is None:
                        break
                    if not self.start_backup(new_instance):
                        instance.stop_timer()
                        log(new_instance.count_str + ' ' + self.type + ': queueing ' + new_instance.name_id +
                            ' for copy', 'debug')
                        self.image_copier.queue.enqueue(new_instance)
                        self.queue.clear_slot(new_instance)
                    else:
                        break


class ImageCopier(Copier):
    """Copies up to 3 ami images concurrently, then queues it for a purge"""

    def __init__(self, imagepurge_obj):
        self.queue = Queue(3)
        self.image_purger = imagepurge_obj
        self.backup_count = 0
        self.boto_conn = None
        # run this backup or skip it
        self.active = True
        self.type = 'backup'

        # figure out if this is a daily, weekly, and/or monthly
        # every run is a daily backup
        self.default_backup_type = ['Daily']
        if datetime.date.today().weekday() == 6:
            # sunday, so a weekly backup
            self.default_backup_type.append('Weekly')
        if datetime.date.today().day == 1:
            # first of month, so a monthly backup
            self.default_backup_type.append('Monthly')
        self.default_backup_type = ','.join(self.default_backup_type)
        # log('backup type: ' + self.default_backup_type, 'debug')

    def check_slots(self):
        self.queue.fill_slots()
        for instance in self.queue.get_slots():
            needs_new_instance = False
            if not self.active:
                log(instance.count_str + ' ' + self.type + ': queueing ' + instance.name_id + ' for purge', 'debug')
                self.image_purger.queue.enqueue(instance)
                self.queue.clear_slot(instance)
                continue
            if instance.status == 'new':
                if not self.start_copy(instance):
                    instance.stop_timer()
                    log(instance.count_str + ' ' + self.type + ': queueing ' + instance.name_id + ' for purge', 'debug')
                    self.image_purger.queue.enqueue(instance)
                    self.queue.clear_slot(instance)
                    needs_new_instance = True
                time.sleep(0.5)
            elif instance.status == 'waiting':
                copy_status = self.is_copy_done(instance)
                if copy_status == 'done':
                    self.tag_new_image(instance)
                    log(instance.count_str + ' ' + self.type + ': queueing ' + instance.name_id + ' for purge', 'debug')
                    self.image_purger.queue.enqueue(instance)
                    self.stop_backup(instance)
                    self.queue.clear_slot(instance)
                    needs_new_instance = True
                elif copy_status == 'timed_out':
                    log(instance.count_str + ' ' + self.type + ': queueing ' + instance.name_id + ' for purge', 'debug')
                    self.image_purger.queue.enqueue(instance)
                    self.stop_backup(instance)
                    self.queue.clear_slot(instance)
                    needs_new_instance = True
                elif copy_status == 'failed':
                    log(instance.count_str + ' ' + self.type + ': removing failed ami for ' + instance.name_id,
                        'debug')
                    self.remove_ami(instance)
                    instance.stop_timer()
                    log(instance.count_str + ' ' + self.type + ': restarting copy for ' + instance.name_id, 'debug')
                    if not self.start_copy(instance):
                        instance.stop_timer()
                        log(instance.count_str + ' ' + self.type + ': queueing ' + instance.name_id + ' for purge',
                            'debug')
                        self.image_purger.queue.enqueue(instance)
                        self.queue.clear_slot(instance)
                        needs_new_instance = True
                time.sleep(0.5)
            if needs_new_instance:
                while 1 == 1:
                    new_instance = self.queue.get_new_slot()
                    if new_instance is None:
                        break
                    if not self.start_copy(new_instance):
                        instance.stop_timer()
                        log(new_instance.count_str + ' ' + self.type + ': queueing ' + new_instance.name_id +
                            ' for purge', 'debug')
                        self.image_purger.queue.enqueue(new_instance)
                        self.queue.clear_slot(new_instance)
                        time.sleep(0.5)
                    else:
                        break


log('backup script version ' + script_version + ' running', 'notice')

image_purge = ImagePurge()
image_copier = ImageCopier(imagepurge_obj=image_purge)
image_creator = ImageCreator(imagecopier_obj=image_copier, imagepurge_obj=image_purge)

primary_region_endpoint = "ec2." + ec2_backup_config.primary_region + ".amazonaws.com"
backup_region_endpoint = "ec2." + ec2_backup_config.backup_region + ".amazonaws.com"

# create 'today string' that will be used for image names
time_str = datetime.date.today().isoformat()

# create the today timestamp that will be used for purges
today_stamp = str(calendar.timegm(datetime.date.today().timetuple()))

# create a region object and connection for the primary, then connection to ec2
try:
    log('connecting to primary ec2 region ' + ec2_backup_config.primary_region + ' using endpoint ' +
        primary_region_endpoint, 'debug')
    region = RegionInfo(name=ec2_backup_config.primary_region, endpoint=primary_region_endpoint)
    pri_reg_conn = boto.connect_ec2(ec2_backup_config.access_id, ec2_backup_config.access_secret, port=443,
                                    region=region)
    image_creator.boto_conn = pri_reg_conn
    image_purge.boto_conns.append(pri_reg_conn)
except EC2ResponseError as e:
    # no recovery from this one
    error_report(
        'could not create primary region object ' + ec2_backup_config.primary_region + ': ' + str(e.error_code),
        True)
except:
    # no recovery from this one
    error_report(
        'could not create primary region object ' + ec2_backup_config.primary_region + ': ' + str(sys.exc_info()[0]),
        True)

if ec2_backup_config.backup_region == '':
    image_copier.active = False
    log('no backup region in config', 'notice')

if image_copier.active:
    # create a region object and connection for the secondary, then connection to ec2
    try:
        region = RegionInfo(name=ec2_backup_config.backup_region, endpoint=backup_region_endpoint)
        log('connecting to backup ec2 region ' + ec2_backup_config.backup_region + ' using endpoint ' +
            backup_region_endpoint, 'debug')
        bu_reg_conn = boto.connect_ec2(ec2_backup_config.access_id, ec2_backup_config.access_secret, port=443,
                                       region=region)
        image_copier.boto_conn = bu_reg_conn
        image_purge.boto_conns.append(bu_reg_conn)
    except EC2ResponseError as e:
        # no recovery from this one
        error_report(
            'could not create backup region object ' + ec2_backup_config.backup_region + ': ' + str(e.error_code),
            True)
    except:
        # no recovery from this one
        error_report(
            'could not create backup region object ' + ec2_backup_config.backup_region + ': ' + str(sys.exc_info()[0]),
            True)

# get list of all instances that are running
# valid values for state are (pending | running | shutting-down | terminated | stopping | stopped)
log('getting running instance list', 'debug')

# the get_all_instances call is deprecated so we've switched to get_only_instances per boto docs
# reservations = pri_reg_conn.get_all_instances(filters={"instance-state-name" : "running"})

try:
    instance_start_time = time.time()
    instances = pri_reg_conn.get_only_instances()
    instance_end_time = time.time()
    instance_total_time = instance_end_time - instance_start_time
except EC2ResponseError as e:
    # no recovery from this one
    error_report('could not retrieve instance list: ' + str(e.error_code), True)
except:
    # no recovery from this one either
    error_report('could not retrieve instance list: ' + str(sys.exc_info()[0]), True)

log('found ' + str(len(instances)) + ' potential instances to backup in ' + secs_to_str(instance_total_time), 'debug')

# this flag is to send to monitoring if there are any backup problems
run_has_errors = False

# the error message to be sent to zabbix for alerts
error_message = ''

# backup count to be sent to zabbix for reporting purposes
backup_count = 0

# running instance count for count_str
instance_count = 0

# loop through instance names in case this is a test
# this also lets us get a count of instances to be backed up
for i in instances:
    # try to get the name of the instance
    try:
        instance_name = i.tags['Name']
    except:
        instance_name = 'no name tag: ' + i.id

    # handle a test run
    if test_all_hosts:
        if instance_backup(instance_name=instance_name, i=i, debug=True, logging=True):
            instance_count += 1
        continue

    # check to see if this is a host to backup.  if not, continue to next
    if not instance_backup(instance_name=instance_name, i=i, debug=False, logging=False):
        continue

    instance_count += 1

if test_all_hosts:
    print 'completed running.  would back up ' + str(instance_count) + ' of ' + str(len(instances)) + ' instances'
    exit(0)

count_str = '-'

# first loop through the instances
#   part 1 is to back them up in the primary region
#   part 2 is to copy to the backup region
for i in instances:

    # try to get the name of the instance
    try:
        instance_name = i.tags['Name']
    except:
        instance_name = 'no name tag: ' + i.id

    # check to see if this is a host to backup.  if not, continue to next
    if not instance_backup(instance_name=instance_name, i=i, debug=False, logging=True):
        continue

    backup_count += 1

    instance = Instance(i)

    instance_name_sub = re.sub(r'[^A-Za-z 0-9+-=._:/@]', '_', instance_name)
    if instance_name is not instance_name_sub:
        log(count_str + ' instance ' + instance_name + ' will be renamed to ' + instance_name_sub, 'notice')
        instance_name = instance_name_sub
    instance.name = instance_name
    instance.name64 = base64.b64encode(instance_name)
    instance.id = i.id
    instance.name_id = instance_name + ' (' + instance.id + ')'
    instance.region = ec2_backup_config.primary_region
    instance.image_name = instance_name + ' BU ' + time_str
    instance.image_name64 = base64.b64encode(instance.image_name)
    instance.image_desc = 'Automated backup of ' + instance_name + ' from ' + ec2_backup_config.primary_region + \
                          ' on ' + time_str
    instance.image_desc64 = base64.b64encode(instance.image_desc)
    instance.today_stamp = today_stamp
    instance.count_str = '(' + str(backup_count) + '/' + str(instance_count) + ')'

    image_creator.queue.enqueue(instance)

creator_start = time.time()
status_time = time.time()
while 1 == 1:
    image_creator.check_slots()
    image_copier.check_slots()
    image_purge.check_slots()
    if image_creator.queue.total_queue_size() == 0:
        if image_copier.queue.total_queue_size() == 0:
            if image_purge.queue.total_queue_size() == 0:
                break

    # log a queue update every 5 minutes or so
    if (time.time() - status_time) > 300:
        elapsed_time = secs_to_str(time.time() - creator_start)
        log('queue status at ' + elapsed_time + ': creator: ' + str(image_creator.queue.total_queue_size()) +
            ', copier: ' + str(image_copier.queue.total_queue_size()) +
            ', purger: ' + str(image_purge.queue.total_queue_size()) +
            ', completed: ' + str(completed_backups), priority='notice')
        status_time = time.time()

    time.sleep(10)

elapsed_time = secs_to_str(time.time() - creator_start)
log('queue status at ' + elapsed_time + ': creator: ' + str(image_creator.queue.total_queue_size()) +
    ', copier: ' + str(image_copier.queue.total_queue_size()) +
    ', purger: ' + str(image_purge.queue.total_queue_size()) +
    ', completed: ' + str(completed_backups), priority='debug')

backup_end_time = time.time()
backup_total_time = backup_end_time - backup_start_time

log('completed running', 'notice')
log('backed up ' + str(backup_count) + ' out of ' + str(len(instances)) + ' instances', 'notice')
log('error count  . . . . . . . . ' + str(error_count), 'notice')
log('failed copies  . . . . . . . ' + str(failed_copies), 'notice')
log('total backup run time  . . . ' + secs_to_str(backup_total_time), 'notice')
log('total instance backup time . ' + secs_to_str(total_instance_time), 'notice')
log('time saved . . . . . . . . . ' + secs_to_str(total_instance_time - backup_total_time), 'notice')

try:
    if not ec2_backup_config.use_zabbix:
        exit(0)
except AttributeError:
    exit(0)

stats_time = str(int(time.time()))
zabbix_stats = open('/tmp/backup_stats', 'w')
zabbix_stats.truncate()
zabbix_stats.write('- aws_bu.bu_count ' + stats_time + ' ' + str(backup_count) + "\n")
zabbix_stats.write('- aws_bu.instance_count ' + stats_time + ' ' + str(len(instances)) + "\n")
zabbix_stats.write('- aws_bu.error_count ' + stats_time + ' ' + str(error_count) + "\n")
zabbix_stats.write('- aws_bu.failed_copies ' + stats_time + ' ' + str(failed_copies) + "\n")
zabbix_stats.write('- aws_bu.run_time ' + stats_time + ' ' + str(int(backup_total_time)) + "\n")
zabbix_stats.close()
zabbix_server = ec2_backup_config.zabbix_server
zabbix_host = ec2_backup_config.zabbix_hostname
zabbix_cmd = '/usr/bin/zabbix_sender -z ' + zabbix_server + ' -s ' + zabbix_host + ' -T -i /tmp/backup_stats'
proc = subprocess.Popen([zabbix_cmd], stdout=subprocess.PIPE, shell=True)
(out, err) = proc.communicate()
log('sent stats to zabbix', 'notice')
log('stdout: ' + str(out), 'notice')
log('stderr: ' + str(err), 'notice')

# pprint(i.__dict__)

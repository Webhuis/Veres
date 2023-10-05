#!/usr/bin/env python3

from multiprocessing import Lock, Process, Queue, current_process, cpu_count
from subprocess import Popen, PIPE

#import subprocess as sp
from threading import Thread   # currentThread is not used
#import multiprocessing as mp
import loguru as log
import os
#import pymysql as pm
import queue as q
import time as t
import datetime as dt
import sys

import classes_veres as vr
#import util_classes_veres as vu

# from Queue import Queue Not used here

# Toepassing objecten verkort de code voor de output files
#
#    #######  #####  ######   #####
#       #    #     # #     # #     #
#       #    #     # #     # #     #
#       #    #     # #     # #     #
#       #    #     # #     # #     #
#       #    #     # #     # #     #
#       #     #####  ######   #####
#
#ToDo
#- Verwerken Spreadsheet
#- Toevoegen foutafhandeling in verzamelscript
#- Exec 2> afhandeling
#- Inbouwen controle hosts uit spreadsheet
#- Opzetten deploy script, waar moeten deze scripts staan?
# innodb_flush_log_at_trx_commit = 2 ?
# innodb_flush_log_at_trx_commit
# In the event of a crash, both 0 and 2 can lose once second of data.
# The tradeoff is that both 0 and 2 increase write performance.
# I choose 0 over 2 because 0 flushes the InnoDB Log Buffer to the Transaction Logs (ib_logfile0, ib_logfile1) once per second, with or without a commit. Setting 2 flushes the InnoDB Log Buffer only on commit. There are other advantages to setting 0 mentioned by @jynus, a former Percona instructor.
# Restart mysql like this

#log = loglib.Logger(maxlines = 0, linelen = 0)
host_objects = {}
db_objects = {}
report = {}

number_of_processes = 4
task_in = Queue()
task_out = Queue()
processes = []

log.logger.add('error.log', filter = lambda record: 'error' in record['extra'] )
error_log = log.logger.bind(error = True)
log.logger.add('ticket.log', filter = lambda record: 'ticket' in record['extra'] )
ticket_log = log.logger.bind(ticket = True)
log.logger.add('veres.log', filter = lambda record: 'veres' in record['extra'] )
veres_log = log.logger.bind(veres = True)

live_run = False

'''
Start of navigation.
'''

def main():

  '''
  '''

  #restore_in = r90_init_main()
  #print(restore_in)
  #r91_db_data_collect()
  work_list = r92_host_db_queue()


  '''
  For the run part:
  Eat your vitamins, do your exercise and say your prayers
  '''

  #r95_process_queues()

  r99_exit_main()

'''
End of navigaton
'''

'''
The nuclear section.
'''

def doit(backup_id, db_brand, db_brand_version, host, domain, db_name, backup_name, backup_location):

  db_brand_literal = r'{}'.format(db_brand)
  try:
    database_object = vr.Database(backup_id, db_brand, db_brand_version, host, db_name, backup_name, backup_location)
    veres_log.info('Database object attributes {}'.format(database_object.toon()))
  except Exception as e:
    error_log.error('Aanmaken {} {} mislukt!\n{}'.format(host, db_name, e.args))
    sys.exit(1)

  try:
    db_conn = database_object.get_connection(host="localhost", user="veres", port=3306, password="veres", database="mysql")
    veres_log.info('database connection db met id {}'.format(id(db_conn)))
  except Exception as e:
    error_log.error('Database server connection {} {} failed!\n{}'.format(host, db_name, e.args))
    sys.exit(1)

  try:
    db_cursor = database_object.get_cursor(db_conn)
    veres_log.info('db_cursor with id {}'.format(id(db_cursor)))
  except Exception as e:
    error_log.error('Database server connection {} {} failed!\n{}'.format(host, db_name, e.args))
    sys.exit(1)

  try:
    new_db = database_object.set_create_database(db_cursor, db_name)
    veres_log.info('Database with name {} created with id {}'.format(db_name, id(new_db)))
  except Exception as e:
    error_log.error('Create {} {} failed!\n{}'.format(db_name, e.args))
    #sys.exit(1)

  try:
    use_db = database_object.set_use_database(db_cursor, db_name)
    veres_log.info('Use database with name {} with id {}'.format(db_name, id(use_db)))
  except Exception as e:
    error_log.error('Use {} {} failed!\n{}'.format(db_name, e.args))
    #sys.exit(1)

  try:
    db_settings_file_name = 'dumps/veres_settings.mys'
    with open(db_settings_file_name, 'r') as f:
      command = ['mysql', '-h{}'.format('localhost'), '-u{}'.format('veres'), '-p{}'.format('veres'), db_name]
      load_db = Popen(command, stdin = f)
      stdout, stderr = load_db.communicate()
    veres_log.info('Set restore backup settings')
  except Exception as f:
    error_log.error('Set restore setting failed!')

  try:
    db_dump_file_name = '{}/{}'.format(backup_location, backup_name)
    with open(db_dump_file_name, 'r') as f:
      command = ['mysql', '-h{}'.format('localhost'), '-u{}'.format('veres'), '-p{}'.format('veres'), db_name]
      load_db = Popen(command, stdin = f)
      stdout, stderr = load_db.communicate()
    #load_db.wait()
    veres_log.info('Restore backup with name {} created with id {}'.format(db_dump_file_name, id(load_db)))
  except Exception as f:
    error_log.error('Restore {}/{} failed!\n{}'.format(db_dump_file_name, id(load_db), f.args))
    ticket_log.info('Restore database {} from backup source {} failed!'.format(db_name, db_dump_file_name))
    #sys.exit(1)

  try:
    db_unsettings_file_name = 'dumps/veres_unsettings.mys'
    with open(db_unsettings_file_name, 'r') as f:
      command = ['mysql', '-h{}'.format('localhost'), '-u{}'.format('veres'), '-p{}'.format('veres'), db_name]
      load_db = Popen(command, stdin = f)
      stdout, stderr = load_db.communicate()
    veres_log.info('Restore normal settings')
  except Exception as f:
    error_log.error('Restore normal setting failed!')

  try:
    tables_db = database_object.get_list_tables(db_cursor)
    veres_log.info('List tables in database with name {} created with id {}'.format(db_name, id(tables_db)))
  except Exception as e:
    error_log.error('List tables in database {} failed!\n{}'.format(db_name, e.args))
    #sys.exit(1)

  if len(tables_db) == 0:
    ticket_log.info('Empty database {}, {}'.format(db_name, db_dump_file_name))
  else:
    veres_log.info('Verifying database {} with {} tabels named:\n{}'.format(db_name, len(tables_db), tables_db))
    for table in tables_db:
      try:
        rows = database_object.get_row_count(db_cursor, db_name, table)
        veres_log.info('Table {} in database {} has {} rows.'.format(table, db_name, rows))
      except Exception as e:
        error_log.error('Select one row failed in on table {} in database {} failed!\n{}'.format(table, db_name, e.args))
        #sys.exit(1)

      try:
        select_row = database_object.get_select_limit(db_cursor, db_name, table)
        print(type(select_row), select_row)
        veres_log.info('Select one row in table {} in database {} gives\n{}.'.format(table, db_name, select_row))
      except Exception as e:
        error_log.error('Select one row failed in on table {} in database {} failed!\n{}'.format(table, db_name, e.args))
        #sys.exit(1)

  try:
    drop_db = database_object.set_drop_database(db_cursor, db_name)
    veres_log.info('Drop database with name {} created with id {}'.format(db_name, id(drop_db)))
  except Exception as e:
    error_log.error('Drop database {} failed!\n{}'.format(db_name, e.args))
    #sys.exit(1)

  try:
    close_cursor = database_object.set_close_cursor(db_cursor)
    veres_log.info('Close cursor.')
  except Exception as e:
    error_log.error('Close cursor {} failed!\n{}'.format(db_name, e.args))
    #sys.exit(1)

  try:
    close_conn = database_object.set_close_connection(db_conn)
    veres_log.info('Close connection with name {}'.format(db_name))
  except Exception as e:
    error_log.error('Close connection {} failed!\n{}'.format(db_name, e.args))
    #sys.exit(1)


'''
Start of move preparation details.
'''

'''
Start of Data Collection.
'''

def r91_db_data_collect():

  '''
  1. Create a list from the spreadsheet
  2. Create a dictionary of hosts in scope
  3. Create dictionary of databases per host
  For the run part:
  Eat your vitamins, do your exercise and say your prayers
  '''

  #restore_in = r80_spreadsheet_list()

  #r81_hosts_db(restore_in)

def r92_host_db_queue():

  '''
  Start of queue preparation.
  '''
  veres_host = '10.68.171.80'
  veres_port = 5432
  veres_db = 'veres'
  backup_schema = 'backups'
  backup_table = 'host_database'
  db_brand = ''
  db_brand_version = ''
  backup_name = ''
  backup_location = ''

  try:
    backup_object = vr.Backup(veres_host, veres_port, veres_db, backup_schema)
    #veres_log.info('Database object attributes {}'.format(database_object.toon()))
    print(id(backup_object))
  except Exception as e:
    print('Aanmaken Backup_object {} {} mislukt!\n{}'.format(veres_host, veres_db, e.args))
    #error_log.error('Aanmaken {} {} mislukt!\n{}'.format(veres_host, veres_db, e.args))
    sys.exit(1)

  try:
    db_conn = backup_object.get_connection(user='veres', password='veres')
    print('database connection db met id {}'.format(id(db_conn)))
    #veres_log.info('database connection db met id {}'.format(id(db_conn)))
  except Exception as e:
    print('Database server connection {} {} failed!\n{}'.format(host, db_name, e.args))
    #error_log.error('Database server connection {} {} failed!\n{}'.format(veres_host, veres_db, e.args))
    sys.exit(1)

  try:
    db_cursor = backup_object.get_cursor(db_conn)
    print('db_cursor with id {}'.format(id(db_cursor)))
    #veres_log.info('db_cursor with id {}'.format(id(db_cursor)))
  except Exception as e:
    error_log.error('Database server connection {} {} failed!\n{}'.format(host, db_name, e.args))
    sys.exit(1)

  try:
    work_list = backup_object.get_work(db_cursor, backup_schema, backup_table)
    #veres_log.info('List tables in database with name {} created with id {}'.format(db_name, id(tables_db)))
    #print(work_list)
  except Exception as e:
    print('List tables in database {} failed!\n{}'.format(db_name, e.args))
    #error_log.error('List tables in database {} failed!\n{}'.format(db_name, e.args))
    #sys.exit(1)

  for db in range(len(work_list)):
    dbt = work_list[db]
    #print(dbt)
    print(dbt[0], dbt[1], dbt[2], dbt[3], dbt[4], dbt[5], dbt[6], dbt[7])
    #task_in.put(doit(dsl[0], dsl[1], dsl[2], dsl[3], dsl[4], dsl[5], dsl[6]))
    #task_in.put(dsl)
    #veres_log.info('Added {}'.format(dsl)

    backup_id = dbt[0]
    db_brand = dbt[1]
    db_brand_version = dbt[2]
    host = dbt[3]
    domain = dbt[4]
    db_name = dbt[5]
    backup_name = dbt[6]
    backup_location = dbt[7]

    try:
      print(backup_id, db_brand, db_brand_version, host, domain, db_name, backup_name, backup_location)
      database_object = vr.Database(backup_id, db_brand, db_brand_version, host, domain, db_name, backup_name, backup_location)
      print('Database object attributes {}'.format(database_object.toon()))
      #veres_log.info('Database object attributes {}'.format(database_object.toon()))
    except Exception as e:
      #error_log.error('Aanmaken {} {} mislukt!\n{}'.format(host, db_name, e.args))
      print('Aanmaken {} {} mislukt!\n{}'.format(host, db_name, e.args))
      sys.exit(1)

def w8ff():

  for w in range(number_of_processes):
    p = Process(target=do_job, args=(task_in, task_out))
    processes.append(p)
    p.start()

  # completing process
  for p in processes:
    p.join()

def do_job(task_in, task_out):
  while True:
    try:
      '''
        try to get task from the queue. get_nowait() function will
        raise queue.Empty exception if the queue is empty.
        queue(False) function would do the same task also.
      '''
      dsl = task_in.get_nowait()
      print(dsl)
      doit(dsl[0], dsl[1], dsl[2], dsl[3], dsl[4], dsl[5], dsl[6])
    except q.Empty:
      break
    except Exception as e:
      error_log.error('do_job {} failed!\n{}'.format(dsl), e.args)

    else:
      '''
        if no exception has been raised, add the task completion
        message to task_that_are_done queue
      '''
      print(dsl, ' is done by ', current_process().name)
      task_out.put(' '.join(dsl) + ' is done by ' + current_process().name)
      #time.sleep(.5)
  return True


def r80_spreadsheet_list():

  '''
  To be replaced with a call to the module that processes the BCM spreadsheat.
  For now we are using a stub.
  '''

def r81_hosts_db(restore_in):

  '''
  The file decription of the file restore_in
  1. Primary nick name of current frame N0999999 for host in scope - van_frame
  1. Fully qualified hostname - host
  '''
  #for db in restore_in:
'''
Common utility routines.
'''

def fetch_object(obj_dict, obj_key):

  object = obj_dict[obj_key][0]

  return object

def dict_update(dict, key, value):

  try:
    dict[key].append(value)
  except:
    dict[key] = [value]

def r90_init_main():

  date_time = str(dt.datetime.now())

  error_log.info(date_time)
  ticket_log.info(date_time)
  veres_log.info('Start run veres: verfication of restored databases')

  input_file='input_coke2.csv'

  try:
    restore_in = open(input_file, 'rt')
    veres_log.info('Open input file {}'.format(input_file))
  except Exception:
    error_log.debug('Open input stream failed!')
    sys.exit(1)

  return restore_in

  print('exit_init_main')

def r99_exit_main():

  print('exit_exit_main')

main()

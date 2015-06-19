__author__ = 'Ben English'
import getopt
import sys
import os
import json
from boto.s3.connection import S3Connection
from boto.s3.key import Key


def find_s3_directory():
    if working_directory.endswith('s3' + os.sep):
        return working_directory

    for f in os.walk(working_directory):
        if 's3' in f[1]:
            s3_directory = working_directory + f[0].replace(working_directory, '').replace(os.sep + os.sep, os.sep) + \
                  's3' + os.sep
            return s3_directory


def transfer_files_to_s3(aws_profile):
    files_to_transfer = []
    for d in os.walk(local_s3_directory):
        for f in d[2]:
            s3_path = d[0].replace(local_s3_directory, config['s3_root']) + os.sep + f
            local_path = d[0] + os.sep + f
            files_to_transfer.append({'local_path': local_path.replace(os.sep + os.sep, os.sep),
                                      's3_path': s3_path.replace(os.sep + os.sep, os.sep)})

    print('==============================================================')
    print('Transferring %s files to s3' % len(files_to_transfer))
    print('From: %s' % local_s3_directory)
    print('To:   s3://%s' % config['bucket'] + '/' + config['s3_root'])
    print('==============================================================')

    s3_conn = S3Connection(profile_name=aws_profile)
    bucket = s3_conn.get_bucket(config['bucket'])
    for f in files_to_transfer:
        print(f['local_path'])
        k = Key(bucket)
        k.key = f['s3_path']
        k.set_contents_from_filename(f['local_path'])


if __name__ == '__main__':

    try:
        opts, args = getopt.getopt(sys.argv[1:], 'hd:p:', ['help', 'working_directory=', 'profile='])
    except getopt.GetoptError as e:
        print(e)
        sys.exit(1)

    working_directory = os.getcwd()
    profile = 'dev'

    for opt, arg in opts:
        if opt in ('-h', '--help'):
            print('Usage:')
            print('-d <project_working_directory>')
            print('--working_directory=<project_working_directory>')
            print('-p <aws_profile>')
            print('--profile=<aws_profile>')
            sys.exit()
        elif opt in ('-d', '--working_directory'):
            working_directory = arg
        elif opt in ('-p', '--profile'):
            profile = arg

    if working_directory[-1] not in os.sep:
        working_directory += os.sep

    local_s3_directory = find_s3_directory()
    config_path = local_s3_directory + 's3_config.json'
    with open(config_path, 'r') as cf:
        config = json.load(cf)

    transfer_files_to_s3(profile)




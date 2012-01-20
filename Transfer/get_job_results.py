#!/usr/bin/env python

import os

report_dir = '/home/ubuntu/Reports'
history_dir = '/home/ubuntu/Reports/History'
logs_dir = '/usr/local/hadoop/logs/history/done/'

if not os.path.exists(report_dir):
    os.mkdir(report_dir)

for root, dirnames, filenames in os.walk(logs_dir):
    if len(filenames):
        for file in filenames:
            dummy = os.popen('cp %s/%s %s' % (root, file, history_dir)).readlines()

os.chdir(report_dir)
dummy = os.popen('tar -cf reports.tar *')

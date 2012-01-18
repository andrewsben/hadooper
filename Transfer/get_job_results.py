#!/usr/bin/env python

import os

report_dir = '/home/ubuntu/Reports/'
dfsio_dir = '/home/hduser/TestDFSIO/'
nn_dir = '/home/hduser/NNOutput/'
mr_dir = '/home/hduser/MRBench'

check_dirs = [dfsio_dir, nn_dir, mr_dir]

for dir_to_check in check_dirs:
    for root, dirnames, filenames in os.walk(dir_to_check):
        if len(filenames):
            for file in filenames:
                dummy = os.popen('cp %s/%s %s/%s_%s' % (root, file, report_dir, root.replace('/home/hduser/','').replace('/', '_'), file)).readlines()

os.chdir(report_dir)
dummy = os.popen('tar -cf reports.tar *')

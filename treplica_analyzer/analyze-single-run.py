#!/usr/bin/python

import sys
from optparse import OptionParser
from math import sqrt

usage = "Usage: %prog [OPTIONS] <data files>\nTry --help for more help."
description = ("Processes and analyzes data produced by a single run" +
               " of the Treplica load generator. Data comes from the" +
               " files produced by all nodes. The final result is" + 
               " a time histogram (and associated GnuPlot files) and" +
               " a summary file that can be further analyzed.")
parser = OptionParser(usage=usage, description=description)
parser.disable_interspersed_args()
parser.add_option("-b", "--bucket-size", type="int", default=1,
            metavar="SIZE",
            help="Sets the bucket size for the histogram in seconds." +
                 " Default: %default")
parser.add_option("-i", "--initial-transient", type="int", default=0,
            metavar="TIME",
            help="The duration of the initial transient period to be ignored" +
                 " in seconds. Default: %default")
parser.add_option("-s", "--span", type="int", default=-1,
            metavar="TIME",
            help="The time span to be analyzed in seconds. This interval" +
                 " starts after the initial transient period and ends when" + 
                 " the provided time has elapsed or the data ends, whichever" +
                 " comes first. If negative, the span covers all remaining" +
                 " data. Default: %default")
parser.add_option("-f", "--file-prefix", type="str", default="single-run",
            metavar="PREFIX",
            help="Sets a prefix for the name of the files created" +
                 " including the path. Default: %default")
parser.add_option("-d", "--data-index", type=int, default=0,
            metavar="INDEX",
            help="Sets an index for the summary data line." +
                 " Default: %default")
parser.add_option("-a", "--append-summary", type="str",
            metavar="FILE",
            help="Appends the summary data line created to FILE, instead of" +
                 " creating a .summary file. Default: %default")

(options, args) = parser.parse_args()

if options.bucket_size < 1:
    parser.error("bucket size must be positive")
if options.initial_transient < 0:
    parser.error("initial transient must be non-negative")
if len(args) == 0:
    parser.error("data files missing")


bucket = options.bucket_size
time_span = 0
op_counter = 0
intervals = {}
r_times = []
op_starts = []
op_ends = []

def get(n):
    if n in intervals:
        return intervals[n]
    return [0, 0, 0]

def mean(list):
    sum = 0
    for item in list:
        sum = sum + item
    return sum / float(len(list))

def median(list):
    list.sort
    return list[len(list) // 2]

def var(list):
    mn = mean(list)
    ss = 0;
    for i in range(len(list)):
        ss += (list[i] - mn) * (list[i] - mn)
    return ss / float(len(list) - 1)

def stdev(list):
    return sqrt(var(list))
    

for arg in args:
    file = open(arg)
    for line in file:
        l = line.split()
        (start, end) = (int(l[0]), int(l[1]))
        time = (end - start)
        start //= bucket * 1000
        end //= bucket * 1000
        start_interval = get(start)
        start_interval[0] += 1
        intervals[start] = start_interval
        end_interval = get(end)
        end_interval[1] += 1
        end_interval[2] += time
        intervals[end] = end_interval

keys = sorted(intervals.keys())
if len(keys) > 0:
    first = keys[0]
    while len(keys) > 0 and (keys[0] - first) * bucket < options.initial_transient:
        keys.pop(0)
        
if len(keys) > 0:
    first = keys[0]
    if options.span >= 0:
        while len(keys) > 0 and (keys[-1] - first) * bucket >= options.span:
            keys.pop()

if len(keys) > 0:
    histogram_file = options.file_prefix + ".histogram"
    file = open(histogram_file, "w")
    last = keys[-1]
    time_span = (last - first + 1) * bucket
    for start in range(first, last + 1):
        interval = get(start)
        index = (start - first) * bucket
        op_started = interval[0] / float(bucket)
        op_starts.append(op_started)
        op_ended = interval[1] / float(bucket)
        op_ends.append(op_ended)
        op_counter += interval[1]
        if interval[1] != 0:
            r_time = interval[2] / float(interval[1])
            r_times.append(r_time)
        else:
            r_time = 0
        file.write(str(index) + " " + str(op_started) + " " + str(op_ended) + " " + str(r_time) + "\n")
    file.close()

    file = open(options.file_prefix + ".plot", "w")
    file.write('plot "' + histogram_file + '" using 1:2 title "Operations Started / s" with lines, \\\n')
    file.write('     "' + histogram_file + '" using 1:3 title "Operations Ended / s" with lines, \\\n')
    file.write('     "' + histogram_file + '" using 1:4 title "Average Response Time" with lines')
    file.close()

    mean_op_ends = mean(op_ends)
    stdev_op_ends = stdev(op_ends)
    mean_r_times = mean(r_times)
    stdev_r_times = stdev(r_times)
    
    print("Time span (s):", time_span)
    print("Total operations:", op_counter)
    print("Operations started (op/s):")
    print("   Average:", mean(op_starts), "Stdev:", stdev(op_starts), "  Median:", median(op_starts))
    print("Operations completed (op/s):")
    print("   Average:", mean_op_ends, "Stdev:", stdev_op_ends, "  Median:", median(op_ends))
    print("Response time (ms):")
    print("   Average:", mean_r_times, "Stdev:", stdev_r_times, "  Median:", median(r_times))
    
    if options.append_summary == None:
        file = open(options.file_prefix + ".summary", "w")
    else:
        file = open(options.append_summary, "a")
    file.write(str(options.data_index) + " " + str(mean_op_ends) + " " + str(stdev_op_ends) + " " + str(mean_r_times) + " " + str(stdev_r_times) + "\n")
    file.close()
else:
    print("Empty data set!")

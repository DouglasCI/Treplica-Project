#!/usr/bin/python

import sys
from optparse import OptionParser
from math import sqrt

usage = "Usage: %prog [OPTIONS] <data files>\nTry --help for more help."
description = ("Computes the average and standard deviation of a set of" +
               " summary files produced by the analyze-* scripts and" +
               " creates the summary of the summaries.")
parser = OptionParser(usage=usage, description=description)
parser.disable_interspersed_args()
parser.add_option("-c", "--columns", type="str",
            metavar="COLUMN[,COLUMN[,...]]",
            help="Selects a subset of the columns to be summarized. Columns" +
                 " are indexed starting from 1 (column 0 is the data index)." +
                 " Default: all")
parser.add_option("-f", "--file-prefix", type="str", default="summarized",
            metavar="PREFIX",
            help="Sets a prefix for the name of the files created including" +
                 " the path. Default: %default")

(options, args) = parser.parse_args()

columns = []
if options.columns != None:
    for column in options.columns.split(","):
        columns.append(int(column))

data = {}

def mean(list):
    sum = 0
    for item in list:
        sum = sum + item
    return sum / float(len(list))

def median(list):
    list.sort
    return list[len(list) / 2]

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
        split_line = line.split()
        index = int(split_line[0])
        if index in data:
            data_line = data[index]
        else:
            data_line = []
            if len(columns) == 0:
                for i in range(1, len(split_line)):
                    columns.append(i)
            for i in range(0, len(columns)):
                data_line.append([])
            data[index] = data_line
        i = 0
        for column in columns:
            data_line[i].append(float(split_line[column]))
            i += 1

keys = data.keys()
keys.sort()
if len(keys) > 0:
    summary_file = options.file_prefix + ".summary"
    file = open(summary_file, "w")
    for key in keys:
        line = str(key)
        for summary in data[key]:
            line += " " + str(mean(summary))
            line += " " + str(stdev(summary))
        print >>file, line
    file.close()
    
    plot = "plot "
    for i in range(0, len(columns)):
        position = (i + 1) * 2
        plot += ('"' + summary_file + '" using 1:' + str(position) + ':' +
                 str(position + 1) + ' title "Column ' + str(position - 1) +
                 '" with errorlines, ')
    plot = plot[:-2]
    file = open(options.file_prefix + ".plot", "w")
    print >>file, plot
    file.close()
else:
    print("Empty data set!")

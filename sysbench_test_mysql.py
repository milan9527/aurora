#!/usr/bin/python
#-*-coding: utf-8-*-

# Script Description:
#-----------------------------------------------------------------------------------------------------
# mysql sysbench test by Young
#-----------------------------------------------------------------------------------------------------

import sys
import os
import os.path
import time
import logging
import commands
import glob
import random
import string
import traceback

# -----------------------------------------------------------------------------------------------------
# System define variables
# -----------------------------------------------------------------------------------------------------
v_time_now    = time.strftime("%Y%m%d_%H%M%S")
BaseName      = os.path.basename(__file__).split('.')[0]
scripts_path  = os.path.dirname(os.path.abspath(__file__))
logs_path     = scripts_path + '/logs'
logFile       = logs_path + "/" + BaseName + ".log." + v_time_now

class Logger:
    def __init__(self, logfile, clevel, Flevel):
        self.logger = logging.getLogger(logfile)
        self.logger.setLevel(logging.DEBUG)
        fmt = logging.Formatter('[%(asctime)s] [%(levelname)s] %(message)s', '%Y-%m-%d %H:%M:%S')

        #Console print
        sh = logging.StreamHandler()
        sh.setFormatter(fmt)
        sh.setLevel(clevel)

        #Log file print
        fh = logging.FileHandler(logfile)
        fh.setFormatter(fmt)
        fh.setLevel(Flevel)
        self.logger.addHandler(sh)
        self.logger.addHandler(fh)

    def debug(self,message):
        self.logger.debug(message)
    def info(self,message):
        self.logger.info(message)
    def war(self,message):
        self.logger.warn(message)
    def error(self,message):
        self.logger.error(message)


def getNow():
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

# -----------------------------------------------------------------------------------------------------
# User define variables
# -----------------------------------------------------------------------------------------------------
threadList = [16,32,64,128,256,512,1024]
clusterList = [
    {'version':'Polardb8.0','size':'polar.mysql.x8.xlarge','endpoint':'pc-2ze8qn46b92xxy830.mysql.polardb.rds.aliyuncs.com'}
    ]

mysqlPort = 3306
mysqlUser = 'milan'
mysqlPassword = 'password'
dbName = 'sysbench_test'
tableNum = 250
tableSize = 25000
combo = ['oltp_read_write','oltp_read_only','oltp_write_only']
#combo = ['oltp_read_write']
outputFile = './result_sysbench_mysql.{}.csv'.format(time.strftime("%Y%m%d%H%M", time.localtime()))

#-----------------------------------------------------------------------------------------------------
# Functions
#-----------------------------------------------------------------------------------------------------
def sysbench_test():
    #create database and prepare data
    for cluster in clusterList:
        LoggerPrint.info("Prepare data for {}".format(cluster['endpoint']))

        # create database
        createDBCommand = "mysql -h{} -P{} -u{} -p{} -e 'drop database if exists {}; create database {} ;'".format(cluster['endpoint'],mysqlPort,mysqlUser,mysqlPassword,dbName,dbName)
        (commandStatus, commandOutput) = commands.getstatusoutput(createDBCommand)
        #LoggerPrint.error(createDBCommand)
        if commandStatus != 0 or  "FATAL:" in commandOutput:
            LoggerPrint.error("Failed to create database by command: {}".format(createDBCommand))
            LoggerPrint.error(commandOutput)
            sys.exit(1)

        # check parameter
        sql = "show variables like 'log_bin'; show variables like 'sync_binlog'; show variables like 'innodb_flush_log_at_trx_commit';"
        checkParCommand = 'mysql -N -s -h{} -P{} -u{} -p{} -e "{}"'.format(cluster['endpoint'],mysqlPort,mysqlUser,mysqlPassword,sql)
        (commandStatus, commandOutput) = commands.getstatusoutput(checkParCommand)
        LoggerPrint.info(commandOutput.replace('mysql: [Warning] Using a password on the command line interface can be insecure.','Key parameters:'))
        if commandStatus != 0 or  "FATAL:" in commandOutput:
            LoggerPrint.error("Failed to check parameter by command: {}".format(checkParCommand))
            sys.exit(1)

        # load data
        prepareCommand = '''sysbench \
                            --mysql-host={} \
                            --mysql-port={} \
                            --mysql-db={} \
                            --mysql-user={} \
                            --mysql-password={} \
                            --table_size={} \
                            --tables={} \
                            --threads=16 \
                            oltp_read_write \
                            prepare'''.format(cluster['endpoint'],mysqlPort,dbName,mysqlUser,mysqlPassword,tableSize,tableNum)
        (commandStatus, commandOutput) = commands.getstatusoutput(prepareCommand)
        if commandStatus != 0 or  "FATAL:" in commandOutput:
            LoggerPrint.error(prepareCommand)
            LoggerPrint.error(commandOutput)
            sys.exit(1)
        LoggerPrint.info('')
    # run performace test
    f = open(outputFile, 'w+')
    fileHeader = 'Version,Size,Scenario,Metrics'
    for thread in threadList:
        colName = ',thread-{}'.format(thread)
        fileHeader+= colName
    f.write(fileHeader)
    for cluster in clusterList:
        for scenario in combo:
            lineHeader = '\n{},{},{}'.format(cluster['version'], cluster['size'], scenario)
            lineQueries = '{},QPS'.format(lineHeader)
            lineTrans = '{},TPS'.format(lineHeader)
            lineLatency = '{},Latency/ms'.format(lineHeader)
            for threads in threadList:
                try :
                    LoggerPrint.info("-"*120)
                    LoggerPrint.info("Running test: version {} | size {} | scenario {} | threads {} ".format(cluster['version'],cluster['size'],scenario,threads))
                    testCommand = '''sysbench \
                                    --mysql-host={} \
                                    --mysql-port={} \
                                    --mysql-db={} \
                                    --mysql-user={} \
                                    --mysql-password={} \
                                    --table_size={} \
                                    --tables={} \
                                    --threads={} \
                                    --db-ps-mode=disable \
                                    --time=300 \
                                    --report-interval=30 \
                                    --percentile=95 \
                                    --rand-type=uniform \
                                    --mysql-ignore-errors=1062 \
                                    --skip-trx=1 \
                                    --range_selects=0 \
                                    {} run '''.format(cluster['endpoint'],mysqlPort,dbName,mysqlUser,mysqlPassword,tableSize,tableNum,threads,scenario)

                    #LoggerPrint.info(testCommand)
                    (commandStatus, commandOutput) = commands.getstatusoutput(testCommand)
                    LoggerPrint.info(commandOutput)
                    rowList = commandOutput.split('\n')
                    #res = {'scenario':scenario,'thread':threads,  'qps':0, 'tps':0, 'Latency':''}
                    for row in rowList:
                        #LoggerPrint.info('-->{}'.format(row))
                        if 'transactions:' in row:
                            tps = int(row.split('(')[1].strip(' per sec.)').split('.')[0])
                            LoggerPrint.info('tps: {}'.format(tps))
                            lineTrans += ',{}'.format(tps)
                        elif 'queries:' in row:
                            qps = int(row.split('(')[1].strip(' per sec.)').split('.')[0])
                            LoggerPrint.info('qps: {}'.format(qps))
                            lineQueries += ',{}'.format(qps)
                        elif '95th percentile:' in row:
                            Latency = int(float(row.split(':')[-1].replace(' ','')))
                            lineLatency += ',{}'.format(Latency)
                    #LoggerPrint.info(res)
                    time.sleep(60)
                except Exception,e:
                    LoggerPrint.error('{0}'.format(traceback.print_exc()))
                    exit(0)
            f.write(lineQueries)
            f.write(lineTrans)
            f.write(lineLatency)

# -----------------------------------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------------------------------
def main():
    sysbench_test()
    return 0

if __name__ == '__main__':
    LoggerPrint = Logger( logFile, logging.INFO, logging.DEBUG)
    try:
        LoggerPrint.info('Start script.')
        main()
        LoggerPrint.info('Output File: {}'.format(outputFile))
        LoggerPrint.info('Script complted successfully.')
        sys.exit(0)
    except Exception,e:
        errMsg = "Error found!"
        LoggerPrint.error(errMsg)
        LoggerPrint.error('{}'.format(traceback.format_exc()))

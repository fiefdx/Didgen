# -*- coding: utf-8 -*-

import os
import sys
import time
import getopt
import logging
from multiprocessing import Process, Pipe, Queue

import redis

import logger

LOG = logging.getLogger(__name__)

class test_worker(Process):
    def __init__(self, process_id, tasks, results, end_task_id):
        Process.__init__(self)
        self.process_id = process_id
        self.tasks = tasks
        self.results = results
        self.end_task_id = end_task_id

    def run(self):
        logger.config_logging(logger_name = "worker",
                              file_name = ("didgen_concurrent_write_test_%s" % self.process_id + '.log'), 
                              log_level = "DEBUG", 
                              dir_name = "logs", 
                              day_rotate = False, 
                              when = "D", 
                              interval = 1, 
                              max_size = 20, 
                              backup_count = 5, 
                              console = True)
        LLOG = logging.getLogger("worker")
        LLOG.propagate = False
        success_processed = 0
        total_processed = 0
        start_id = 10000000000000000000L
        end_id = 0
        r = redis.StrictRedis(host = 'localhost', port = 6389, db = 0)
        n = 0
        while True:
            if self.tasks.empty() == False:
                task = self.tasks.get()
                if task != "mission complete":
                    total_processed += 1
                    try:
                        task_id = task[0]
                        gid = r.get("id_test")
                        if gid.isdigit() and int(gid) == 0:
                            LLOG.error("Invalid Id: %s", gid)
                        elif not gid.isdigit():
                            LLOG.error("Invalid Id: %s", gid)
                        if int(gid) < start_id:
                            start_id = int(gid)
                        if int(gid) > end_id:
                            end_id = int(gid)
                        n += 1
                        if n % 5000 == 0:
                            LLOG.debug("Worker %04d: %020d", self.process_id, int(gid))
                        # LLOG.debug("Worker %04d: %020d", self.process_id, int(gid))
                        success_processed += 1
                    except Exception, e:
                        LLOG.exception(e)
                else:
                    break
            else:
                time.sleep(0.0001)
        LLOG.info("total processed: %s, total successed: %s", total_processed, success_processed)
        self.tasks.put("mission complete")
        self.results.put((total_processed, success_processed, start_id, end_id))


if __name__ == "__main__":
    logger.config_logging(
        file_name = "didgen_concurrent_test.log", 
        log_level = "DEBUG", 
        dir_name = "logs", 
        day_rotate = False, 
        when = "D", 
        interval = 1, 
        max_size = 20, 
        backup_count = 5, 
        console = True
    )
    LOG.info("Start didgen_concurrent_test Script")

    test_num = 1000000 + 2000
    process_num = 4
    mission_queue = Queue(10000)
    result_queue = Queue(process_num)

    r = redis.StrictRedis(host = 'localhost', port = 6389, db = 0)
    if r.exists("id_test"):
        r.delete("id_test")
    r.set("id_test", 0)

    process_list = []
    for i in range(process_num):
        p = test_worker(i, mission_queue, result_queue, test_num - 1)
        p.start()
        process_list.append(p)

    t = time.time()
    for i in xrange(test_num):
        while mission_queue.full() == True:
            time.sleep(0.0001)
        mission_queue.put((i, ))
    mission_queue.put('mission complete')
    for i in process_list:
        i.join()
    total_processed = 0
    success_processed = 0
    start_id = 10000000000000000000L
    end_id = 0
    for i in xrange(process_num):
        tmp = result_queue.get()
        total_processed += tmp[0]
        success_processed += tmp[1]
        if tmp[2] > 0 and tmp[2] < start_id:
            start_id = tmp[2]
        if tmp[3] > 0 and tmp[3] > end_id:
            end_id = tmp[3]
    tt = time.time()
    LOG.info("Use Time: %ss", tt - t)
    LOG.info("total processed: %s, %s/s\ntotal successed: %s, %s/s", total_processed,
                                                                     total_processed / (tt-t),
                                                                     success_processed,
                                                                     success_processed / (tt - t))
    LOG.info("Start Id: %s, End Id: %s, Total Should Be: %s", start_id, end_id, end_id - start_id + 1)
    if total_processed != (end_id - start_id + 1):
        LOG.error("Something occured!")
    LOG.info("End didgen_concurrent_test Script")
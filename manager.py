import time

import django_rq
from rq.job import Job

from exceptions import FailedCalcQueueError, RequeueFailedError


class QueueManager(object):

    def __init__(self, code, conn, tasks: list):
        """

        :param code: 作为存储在redis中的key需保证唯一，结构为 {code}_QueueManager
        :param conn: redis coon
        :param tasks: 需要检查的队列名称
        """
        self.code = code
        self.tasks = tasks
        self.queues = {task: django_rq.get_queue(task) for task in self.tasks}
        self.job_ids = {task: [] for task in self.tasks}
        self.jobs = {task: [] for task in self.tasks}
        self.init_statistic = {
            k: {
                "count": self.queues[k].failed_job_registry.count,
                "ids": self.queues[k].failed_job_registry.get_job_ids()
            } for k in self.tasks
        }
        self.failed_task = []
        self.conn = conn

    @staticmethod
    def _is_exists_traceback(job):
        if "Moved to FailedJobRegistry at" in job.exc_info and "Traceback" not in job.exc_info:
            return True
        else:
            raise FailedCalcQueueError()

    def _failed_job_enqueue(self):
        for task, stuck_jobs in self.jobs.items():
            # jobs = Job.fetch_many(failed_job_list, connection=redis)
            for job in stuck_jobs:
                # job = Job.fetch(jid, connection=self.queues[task].connection)
                self.queues[task].enqueue_job(job)

    @property
    def _has_trace_failed_job(self):
        for job_list in self.jobs.values():

            for job in job_list:
                try:
                    self._is_exists_traceback(job)
                except FailedCalcQueueError:
                    return True
        return False

    @property
    def _has_failed_queue(self):
        for task in self.tasks:
            if int(self.queues[task].failed_job_registry.count) != int(self.init_statistic[task]['count']):
                self.failed_task.append(task)
                all_ids = self.queues[task].failed_job_registry.get_job_ids()
                # 归入 FAILED JOBS 的 JOB ID
                new_ids = list(set(all_ids).difference(set(self.init_statistic[task]["ids"])))
                for jid in new_ids:
                    self.job_ids[task].append(jid)
                    self.jobs[task].append(Job.fetch(jid, connection=self.queues[task].connection))
        if self.failed_task:
            return True
        else:
            return False

    @property
    def active_count(self):
        count = 0
        for queue in self.queues:
            count += queue.count or queue.started_job_registry.count
        return count

    def requeue(self):
        self._failed_job_enqueue()

    @property
    # 仅在 requeue 开始前和结束后调用
    def job_status(self):
        if self._has_failed_queue:
            if self._has_trace_failed_job:
                job_status = "invalid"
            else:
                job_status = "stuck"
        else:
            job_status = "finished"

        return job_status

    def _final_check(self):
        while self.active_count:
            time.sleep(5)

        if self._has_failed_queue:
            self.conn.delete(f"{self.code}_QueueManager")
            raise RequeueFailedError()
        else:
            self.conn.delete(f"{self.code}_QueueManager")
            return "finished"

    def start_check(self, simple=True):
        job_status = self.job_status
        print("QueueManager", job_status)
        if simple:
            if job_status == "stuck":
                self.conn.set(f"{self.code}_QueueManager", self.job_ids, timeout=None)
            return job_status

        if job_status == "stuck":
            self.requeue()
            # 检查重新入队后的 job 是否结束
            return self._final_check()

    def recover(self):
        for task, ids in self.job_ids:
            for jid in ids:
                self.jobs[task].append(Job.fetch(jid, connection=self.queues[task].connection))

        self.requeue()
        self._final_check()

        return self.job_status


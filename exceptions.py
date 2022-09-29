# -*- coding: utf-8 -*-
# @Time    : 2022/9/29 15:01
# @Author  : yangshiqi
# @Site    : 
# @File    : exceptions.py
# @Software: PyCharm


class FailedCalcQueueError(Exception):
    """
    任务失败
    """
    pass


class RequeueFailedError(Exception):
    """
    重新入队失败
    """
    pass

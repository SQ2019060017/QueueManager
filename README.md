# QueueManager

# 功能介绍
应用于 Django 框架下，检查 Redis 队列中是否存在掉队任务，即被归入 Failed Jobs 但没有具体报错（traceback），并重新入队

# 示例

```python

# 使用示例

qm = QueueManager(code="project_name", conn=conn, tasks=["task1", "task2"])
# 检查队列运行状态，是否存在失败队列
qm.start_check()
# 对无报错信息的失败任务重新入队，并判断最终运行结果
qm.recover()

```
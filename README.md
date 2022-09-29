# QueueManager

# 功能介绍
应用于 Django 框架下，检查 Redis 队列中是否存在掉队任务，即被归入 Failed Jobs 但没有具体报错（traceback），并重新入队
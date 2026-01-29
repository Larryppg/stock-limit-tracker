import subprocess

# 先重建数据库
subprocess.run(["python", "database.py", "--reset-db"])

# 再运行 backfill 模式
subprocess.run(["python", "main.py", "--mode", "backfill"])
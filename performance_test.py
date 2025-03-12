# -*- coding: utf-8 -*-
import subprocess
import time

NUM_CLIENTS = 40

# クライアントを並列に起動
processes = []
for i in range(NUM_CLIENTS):
    p = subprocess.Popen(
        ["python3", "performance_test_run_client.py", str(i)],
        stdin=subprocess.PIPE,
        stdout=subprocess.DEVNULL,
        # stdout=None,
        stderr=subprocess.DEVNULL,
        # stderr=None,
        # text=True,
    )
    processes.append(p)
    time.sleep(0.1)

for p in processes:
    p.wait()

print("負荷テスト完了！")

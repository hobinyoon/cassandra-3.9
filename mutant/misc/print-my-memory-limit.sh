#!/bin/bash

# http://unix.stackexchange.com/questions/242718/how-to-find-out-how-much-memory-lxc-container-is-allowed-to-consume

cat /sys/fs/cgroup/memory$(cat /proc/self/cgroup | grep memory | cut -d: -f3)/memory.limit_in_bytes

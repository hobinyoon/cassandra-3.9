#!/bin/bash

set -e
set -u
set -x

DN_THIS=`dirname $BASH_SOURCE`

pushd $DN_THIS/../bin >/dev/null

restart_dstat() {
	# Kill dstat and wait until it is terminated
	#   http://stackoverflow.com/questions/17894720/kill-a-process-and-wait-for-the-process-to-exit
	PID_DSTAT_BG=`ps -ef | grep dsta[t] | grep csv | grep date | awk '{print $2}'`
	if [ ! -z "$PID_DSTAT_BG" ]; then
		kill $PID_DSTAT_BG

		while true; do
			kill -0 $PID_DSTAT_BG
			if [ "$?" -ne "0" ]; then
				printf "dstat %d terminated\n" $PID_DSTAT_BG
				break
			fi
			sleep 0.1;
		done
	fi

	dstat -cdn -C total -D xvda,xvdb,xvde,xvdf -r --output /home/ubuntu/work/mutants/log/dstat-`date +\"%y%m%d-%H%M%S\"`.csv >/dev/null 2>&1 &
}

# TODO: keep working on this!
TODO
restart_dstat

./cassandra -f | \grep --color=always -E '(^|^WARN)'

popd >/dev/null

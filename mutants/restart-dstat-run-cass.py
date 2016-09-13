#!/usr/bin/env python

import datetime
import os
import re
import subprocess
import sys
import types

sys.path.insert(0, "%s/work/mutants/ec2-tools/lib/util" % os.path.expanduser("~"))

import Cons
import Util


def main(argv):
	RestartDstat()

	KillExistingCassandra()

	# Load the cgroup config
	Util.RunSubp("sudo cgconfigparser -l %s/cgconfig.conf" % os.path.dirname(__file__))

	# Run Cassandra in the foreground. grep needs to be unbuffered.
	Util.RunSubp("cgexec -g memory:small_mem %s/work/mutants/cassandra/bin/cassandra -f" \
			" | \grep --color=always --line-buffered -E '(^|^WARN)'" \
			% os.path.expanduser("~"))


def RestartDstat():
	with Cons.MT("Restarting dstat ...", print_time=False):
		cmd = "ps -e -o pid,ppid,user,args"
		lines = Util.RunSubp(cmd, print_cmd=False, print_output=False)
		#Cons.P(lines)
		pids = []
		for line in lines.split("\n"):
			line = line.strip()
			if "dstat" not in line:
				continue
			if "csv" not in line:
				continue

			# Get the second-level processes, skipping the root-level ones.
			t = re.split(" +", line)
			if t[1] == "1":
				continue
			pids.append(t[0])
			#Cons.P("[%s]" % line)

		if len(pids) > 0:
			#Cons.P("[%s]" % " ".join(pids))
			Util.RunSubp("kill %s" % " ".join(pids))

			# Make sure each of the processes has terminated
			for pid in pids:
				cmd = "kill -0 %s" % pid
				while True:
					r = 0
					with open(os.devnull, "w") as devnull:
						r = subprocess.Popen(cmd, shell=True, stdin=devnull, stdout=devnull, stderr=devnull)
					if r != 0:
						Cons.P("Process %s has terminated" % pid)
						break
					time.sleep(0.1)

		# Run dstat as a daemon
		fn_out = "%s/work/mutants/log-volatile/dstat/%s.csv" \
				% (os.path.expanduser("~"), datetime.datetime.now().strftime("%y%m%d-%H%M%S"))
		cmd = "dstat -tcdn -C total -D xvda,xvdb,xvdd,xvde,xvdf -r --output %s" % fn_out
		Util.RunDaemon(cmd)


def KillExistingCassandra():
	# Kill any existing instances just in case
	with Cons.MT("Killing existing Cassandra ...", print_time=False):
		cmd = "ps -e -o pid,ppid,user,args"
		lines = Util.RunSubp(cmd, print_cmd=False, print_output=False)
		#Cons.P(lines)
		pids = []
		for line in lines.split("\n"):
			if "/work/mutants/cassandra/bin" not in line:
				continue
			line = line.strip()
			#Cons.P(line)

			# Get the second-level processes, skipping the root-level ones.
			t = re.split(" +", line)
			pids.append(t[0])

		if len(pids) > 0:
			Util.RunSubp("kill %s" % " ".join(pids))

			# Make sure each of the processes has terminated
			for pid in pids:
				cmd = "kill -0 %s" % pid
				while True:
					r = 0
					with open(os.devnull, "w") as devnull:
						r = subprocess.Popen(cmd, shell=True, stdin=devnull, stdout=devnull, stderr=devnull)
					if r != 0:
						Cons.P("Process %s has terminated" % pid)
						break
					time.sleep(0.1)


if __name__ == "__main__":
	sys.exit(main(sys.argv))

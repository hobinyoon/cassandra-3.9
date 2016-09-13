#!/usr/bin/env python

import os
import sys

sys.path.insert(0, "%s/work/mutants/ec2-tools/lib/util" % os.path.expanduser("~"))

import Cons
import Util


def main(argv):
	# Load the cgroup config
	Util.RunSubp("sudo cgconfigparser -l %s/cgconfig.conf" % os.path.dirname(__file__))

	# Run Cassandra in the foreground. grep needs to be unbuffered.
	Util.RunSubp("cgexec -g memory:small_mem %s/print-my-memory-limit.sh"
			% os.path.dirname(__file__))

if __name__ == "__main__":
	sys.exit(main(sys.argv))

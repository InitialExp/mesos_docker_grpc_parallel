#!/bin/bash
mesos-agent --master=172.26.8.130:5050 --work_dir=/root/mesos/agent_dir --executor_environment_variables=/root/mesos/config_files/environment_variables --containerizers=mesos,docker --resources=ports:[31000-32000]

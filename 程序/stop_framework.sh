#!/bin/bash
curl -XPOST http://node1:5050/master/teardown -d "frameworkId=$(cat frameworkId)"

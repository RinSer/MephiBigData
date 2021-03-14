#!/bin/bash

if [[ $# -lt 3 ]] ; then
    echo 'You should specify number of files, number of lines in each and time scale!'
    exit 1
fi

# prints "metricId timestamp value" 
# lines into $1 files

# Metric ids are numbers from 1 to 9
METRIC_IDS=($(seq 1 9))

# Function to return random line with metric value and timestamp
getMetricLine() {
	sleep `echo "scale=2; $RANDOM/$((32767*2))" | bc` # Add randomness to timestamp intervals
	echo "${METRIC_IDS[$((RANDOM % ${#METRIC_IDS[*]}))]}, $(/usr/bin/date +%s), $RANDOM" >> input/metrics.$1
}

rm -rf input
mkdir input

for ((i=1; i<=$1; i++)) do
	for ((j=1; j<=$2; j++)) do
		$(getMetricLine $i)
	done
done

# Prepare hadoop file system
hdfs dfs -rm -r input output

# Send input data to the distributed fs
hdfs dfs -put input input

# Run the application
yarn jar target/lab1-1.0-SNAPSHOT-jar-with-dependencies.jar input output $3
#!/bin/bash
if [[ $# -eq 0 ]] ; then
    echo 'You should specify output file!'
    exit 1
fi

# prints "metricId timestamp value" 
# lines into three files

# Metric ids are numbers from 1 to 9
METRIC_IDS=($(seq 1 9))

# Function to return random line with metric value and timestamp
fileName=$1
getMetricLine() {
	sleep `echo "scale=2; $RANDOM/$((32767*2))" | bc` # Add randomness to timestamp intervals
	echo "${METRIC_IDS[$((RANDOM % ${#METRIC_IDS[*]}))]}, $(/usr/bin/date +%s), $RANDOM" >> input/$fileName.$1
}

rm -rf input
mkdir input

for i in {1..300}
	do 
		$(getMetricLine 1)
	done

for i in {1..300}
	do
		$(getMetricLine 2)
	done

for i in {1..300}
	do
		$(getMetricLine 3)
	done

## This is my attempt at solving the challenge.

solution.py contains the beam implementation.

output.tsv contains the output from the solution.py when run i the 1_day.jsonl file

### Running solution.py
The variable 'datafile' in line 9 should point to the textfile.  
You should be able to run the script with a python interpreter. 

### Assumptions
Well, the output consumption values from the script don't make sense. It looks like the regions have negative net-consumption in some intervals. The reason for this is that each sampled netflow value gets added inside every interval. Since these are sampled every 5 minutes and since they appear to have the exact same value for hours, this is of course wrong. Apparantly we sample the same value over and over again. The assumption I make, however, is that the netflow value is the netflow since last sample - and therefore I just add all these value within the window. To get the numbers to make sense, we would have to know the correct netflow over a time interval.

### What I did
I haven't really used beam before, so I wanted to try it out. I started out with java which is my goto-language, but I figured it could be fun trying with python - it looks quite elegant in comparison. 
Beam makes it easy to parallelize the map-side computations, and also, you can run it on cloud infrastructure on however many machines you'd like. That enables fast parallel execution. 

### What I would do if I had more time
I spent some hours getting to know beam, so I ran out of time. If I had more time, I'd make some tests of helper-functions and make error handling. Then I'd figure out how to get the correct values for the netflow. Then I'd try to learn more about writing production code in python - or I might rewrite it in java where I feel more at home.













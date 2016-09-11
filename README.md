# [HSReplay.net - Replay Analysis](https://hsreplay.net)

Tools for doing large scale analysis on hsreplay.net data.

## Technology overview

Replay analysis jobs are written using Yelp's [MRJob](https://pythonhosted
.org/mrjob/index.html) library to process replays at scale via Map Reduce on EMR. Data 
scientists can easily develop jobs locally and then submit a request to a member of the 
HearthSim team to have them run the job at scale on a production map reduce cluster.

Checkout `yogg_impact.py` for an example of how to write a job that uses a `hearthstone
.hslog.watcher.LogWatcher` subclass to do an analysis against raw power.log files. As a 
data point towards the value of using EMR, running this example job on a set of 150,000 
replays on a single machine was going to take more than 24 hours to complete when 
originally written, however when it was run on EMR that same job only took 35 minutes to 
complete!

Jobs that follow this template will have several things in common:

1. They use the `protocols.PowerlogS3Protocol` input format class which handles 
abstracting away the source of the raw log data.
2. They implement a subclass of `LogWatcher` and use the exposed hooks to capture whatever
event data the job is focused on analyzing.
3. They usually emit their final output as aggregates in a CSV like format so that final 
chart generation and analysis can be done in interactive visual tools like Excel.

### Running A Local Job

To run a job you must first make sure you have the libraries listed in requirements.txt 
installed. Then the command to invoke a job is:
```
$ python <JOB_FILE.PY> <INPUT_FILE.TXT>
```
The INPUT_FILE.TXT must be the path to a text file on the local file system that contains
 newline delimited lines where each line follows the format:
```
<STORAGE_LOCATION>:<FILE_PATH>
```
If STORAGE_LOCATION is the string `local` than the job will look for the file on the 
local file system. If it is any other value, like `hsreplaynet-replays` then it assumes 
that the file is stored in an S3 bucket with that name.

### Example - Running A Local Job

Let's assume that your job script is named `my_job.py` and your input file is named 
`inputs.txt` and looks as follows:
```
local:uploads/2016/09/ex1_power.log
local:uploads/2016/09/ex2_power.log
local:uploads/2016/09/ex3.power.log
```
The `PowerlogS3Protocol` will then look for those files in the `./uploads` directory which 
it expects to be in the same folder as the `my_job.py` script. Once the test data is 
prepared, then the job can be run by invoking:
```
$ python my_job.py inputs.txt
```
This will run the job entirely in a single process which makes it easy to attach a 
debugger or employ any other traditional development practice. In addition, one of the 
benefits of using Map Reduce is that the isolated nature of map() and reduce() functions 
makes them easy to unit test.

### Example - Running An EMR Job

When your job is ready, have a member of the HearthSim team run it on the production data 
set. There are two small changes necessary to make the job run on EMR.
 
1) You must replace the <STORAGE_LOCATION> in `inputs.txt` with the name of the raw log 
data bucket. Usually `hsreplaynet-replays`, so that it looks like:
```
hsreplaynet-replays:uploads/2016/09/ex1_power.log
hsreplaynet-replays:uploads/2016/09/ex2_power.log
hsreplaynet-replays:uploads/2016/09/ex3.power.log
```
Since you likely want to run it on a larger set of inputs, you can ask a member of the 
HearthSim team to help you generate a larger input file by telling them the type of 
replays that you'd like to run the job over.

2) When the HearthSim team member invokes the job they will do so from a machine in the 
data center that is configured with the correct AWS credentials in the environment. They 
will also use the `-r emr` option to tell MRJob to use EMR. E.g.
```
$ python my_job.py -r emr inputs.txt
```
And that's it! MRJob will automatically provision an elastic map reduce cluster, whose 
size can be tuned by a HearthSim member by editing `mrjob.conf` prior to launching the 
job. When the job is done MRJob will either stream the results back to console or save 
them in S3 and then tear down the EMR cluster.

Happy Questing, Adventurer!

### Advanced - Rapid Prototyping For HearthSim Members

When working on the data processing infrastructure it is possible to only pay the cost of
 bootstrapping the cluster once by first running this command:
```
$ mrjob create-cluster --max-hours-idle 1
```
This will create a cluster that will remain active until it's idle for a full hour 
and then it will shut itself down. The command will return a cluster ID token that looks 
like 'j-1CSVCLY28T3EY'.

Then when invoking subsequent jobs the additional `--cluster-id <ID>` command can be used 
to have the job run on the already provisioned cluster. E.g.
```
$ python my_job.py -r emr --cluster-id j-1CSVCLY28T3EY inputs.txt
```
## License

Copyright © HearthSim - All Rights Reserved

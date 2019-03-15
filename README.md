## Distributed MapReduce
This is a interactive command utility to take user inputs to run map-reduce tasks.     

### Building the executables
You should have Java 8 and Apache Maven installed on your system.  
Once those are install, open the terminal and change the directory to project's directory which is distributed-map-reduce  
Run `sh build.sh` or `./build.sh` to let Maven compile the sources and create an executable java archive file

### Using the utility 
You can configure the utility via configuration properties file. I have included a sample config.properties file present at `/src/main/resources/config.properties`.
It is best if you make changes to this file only so that you dont have to update the path to configuration file in run.sh.  
  
The parameters in the configuration file are as follows  
- master : IP address and port of the master node  
- mappers : Comma separated IP addresses and ports of the mapper nodes  
- reducers : Comma separated IP addresses and ports of the mapper nodes  
- input_dir : Parent directory of input files. 
The default value is ./src/main/resources which contains 2 sample file I have uploaded for testing.  
- output_dir : Output directory. The default value is ./output. Program will create this directory in project's root directory if not present.  
  
If you want to test the utility with the files I have provided, dont change any value. Easier for you.   

### Running the utility
Run `sh run.sh` or `./run.sh` to start the utility  
As soon as the utility is stated, it parses configuration file and show those details on command line  
`>>>` is prompted once the control is given to the user. User can interact with the utility only when `>>>` is prompted. 
This is because java does not let the program kill the process programmatically. 
The control will reach the user when the utility has finished processing the command.  
  
The utility support 4 commands  
`init` - to create a cluster. It spawns master and worker threads.  
`run <task-id>` - to run the cluster where the task-ids for word count and inverted intex are 1 and 2 respectively  
`destroy` - to destroy the cluster  
`exit` - to exit the utility prompt
  
`run` command  will create reducer output files in output directory. 
If you used the file I provided you should have outputs similar to below ones.
You can print the output of all files using command `cat output\*.txt` where `output` is the output directory.
The output is consistent with the data I have provided in the files.
```
# for word count
world=4
java=2
hello=1
bye=1

# for inverted index
world={doc1.txt=2, doc2.txt=2}
java={doc1.txt=1, doc2.txt=1}
hello={doc1.txt=1}
bye={doc2.txt=1}
```
  
The logs are provided in log directory.
I have also upload more input files present at `src/test/resources` for additional testing

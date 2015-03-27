# BigBWA
Approaching the Burrows-Wheeler Aligner to Big Data Technologies

# What's BigBWA about? #

**BigBWA** is a tool to run [BWA][1] in a [Hadooop][2] cluster. **BigBWA** splits the [Fastq][3] input reads in pieces and process this pieces in parallel. For now, it supports the following BWA algorithms.

* BWA-MEM paired.
* BWA-ALN paired.

Current version is 0.1.

# Structure #
In this GitHub repository we can find:

* bwa-0.5.10-mt and bwa-0.7.12 folders. These are folders that include bwa versions 0.5.10-mt and 0.7.12 respectivelly. **BigBWA** can be built without these, but it will not work without a shared library made from bwa code.
* libs and libs32. Folders with the Hadoop libraries needed to build **BigBWA**, for 64 and 32 bits respectivelly.
* utils. Python utils to deal with **BigBWA** inputs and outputs.
* BigBWA.java. **BigBWA** main file.
* Build-BigBWA-0.5.10-mt.sh. Script to build **BigBWA** with the bwa 0.5.10-mt version.
* Build-BigBWA-0.7.12.sh. Script to build **BigBWA** with the bwa 0.7.12 version.
* Clean.sh. Script to clean the building.

You can find more info inside each one of the folders.

# How it works? #

## JNI ##
What we intent to do is to call bwa from Java. For that we use JNI. To call C methods from Java with JNI, we need a shared library. Because of that, we modified the bwa Makefile to build this library, which we named libbwa.so. In folders bwa-0.7.12 and bwa-0.5.10-mt we can find the original Makefiles from these versions of bwa modified to build this library.

Also, we need a Java file where define the methods we are going to call. We have put this file also inside the bwa-0.7.12 and bwa-0.5.10-mt folders, and it is named BwaJni.java. With this file, and by calling javah command, we get the BwaJni.h file, where native definitions of methods that are going to be called from Java are. The code of this methods is in bwa_jni.c, also inside bwa folders, and modified Makefile includes this files into bwa build.

So, with this approach, if a new version of bwa is released, we just need to modify Makefile and add this files, and, as long functions names and arguments doesn't change, **BigBWA** will work.

## Utils ##
In the utils folder we can find some Python tools to deal with **BigBWA** inputs and outputs. This utils are needed because of the way Hadoop handles inputs and outputs. More information can be found inside this folder.

## Hadoop ##
To run **BigBWA** we need to upload the libbwa.so shared library into Hadoop distributed cache. For that, we use the -archives Hadoop option with the generated file bwa.zip. This file is created by the build scripts and it is mandatory to use it to run **BigBWA**.

# Getting started #

## Requirements
To build **BigBWA**, requirements are the same than the ones to build bwa, with the only exception that you need to have defined the *JAVA_HOME* environment variables. To build the jar file we are going to run in Hadoop, three hadoop jars are required. This jar files can be found inside libs and libs32 folders. Depending on yout Hadoop installation you should use libs (for 64 bits) or libs32 (for 32 bits). Here follows an example of how to build **BigBWA**:

	git clone https://github.com/citiususc/BigBWA.git
	cd BigBWA
	chmod +x *.sh
	./Build-BigBWA-0.7.12.sh
	
This will generate BigBWA.jar file and bwa.zip file.

## Running BigBWA ##
To run **BigBWA** we need a working Hadoop cluster. Also, in this cluster, we need to ensure that we have available at least 9 GB of free memory per map, because each map is going to load into memory bwa index. We also need to take into account that **BigBWA** uses disk space in Hadoop tmp directory.

Here is an example of how to run **BigBWA** with BWA-MEM paired algorithm. This example assumes that our index is in all cluster nodes at /Data/HumanBase/ .

First we need to have input Fastq reads, we can get them from the 1000 Genomes project ftp.

	wget ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/data/NA12750/sequence_read/ERR000589_1.filt.fastq.gz
	wget ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/data/NA12750/sequence_read/ERR000589_2.filt.fastq.gz
	
Next we will uncompress these files:

	gzip -d ERR000589_1.filt.fastq.gz
	gzip -d ERR000589_2.filt.fastq.gz
	
After that, we need to use one of the Python tools in the utils folder:

	cd utils ; python Fq2FqBigDataPaired.py ../ERR000589_1.filt.fastq ../ERR000589_2.filt.fastq ../ERR000589.fqBD ; cd ..
	
Then, we have to upload input data into HDFS:

	hdfs dfs -copyFromLocal ERR000589.fqBD ERR000589.fqBD
	
And finally, we can run **BigBWA** in Hadoop:

	hadoop jar BigBWA.jar -archives bwa.zip -D mapreduce.input.fileinputformat.split.minsize=123641127 -D mapreduce.input.fileinputformat.split.maxsize=123641127 mem paired /Data/HumanBase/hg19 ERR000589.fqBD ExitERR000589
	
After that, we will have the output in the HDFS. To get it to the local filesystem:

	mkdir Exit
	cd Exit ; hdfs dfs -copyToLocal ExitERR000589/Output* ./ ; cd ..
	
The output will be splited in pieces. If we want to ut it together we can use one of our Python utils or use samtools merge:

	cd utils ; python FullSam.py Exit/ ../OutputFile.sam ; cd ..

##Frequently asked questions (FAQs)

1. [I can not build the tool because *jni_md.h* is missing.](#building1)


####<a name="building1"></a>1. I can not build the tool because *jni_md.h* is missing.
Depending on your Linux distribution you should edit bwa Makefile, either 0.7 or 0.5, and change the line:

	INCLUDES=      -I$(JAVA_HOME)/include/ -fPIC
	
By:

	INCLUDES=	-I$(JAVA_HOME)/include/ -I$(JAVA_HOME)/include/linux/  -fPIC


[1]: https://github.com/lh3/bwa
[2]: https://hadoop.apache.org/
[3]: http://en.wikipedia.org/wiki/FASTQ_format

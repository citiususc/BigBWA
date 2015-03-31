# BigBWA
Approaching the Burrows-Wheeler Aligner to Big Data Technologies

# What's BigBWA about? #

**BigBWA** is a tool to run [BWA][1] in a [Hadooop][2] cluster. **BigBWA** splits the [Fastq][3] input reads in pieces and process this pieces in parallel. For now, it supports the following BWA algorithms.

* BWA-MEM paired.
* BWA-ALN paired.

Current version is 0.1.

# Structure #
In this GitHub repository we can find these directories:

* bwa - In this folder is where we put all the bwa versions software to build **BigBWA** against. Nowadays we include BWA versions 0.5.10-mt and 0.7.12, but, according with our development approach, **BigBWA** will work with later versions of BWA.
* libs - Here are the Hadoop libraries needed to build **BigBWA**. They are built for 64 bits. In the subdirectory 32 we can find the 32 bits libraries.
* src - The **BigBWA** source code.

You can find more info inside each one of the folders.

# Getting started #

## Requirements
To build **BigBWA**, requirements are the same than the ones to build bwa, with the only exception that you need to have defined the *JAVA_HOME* environment variables. If not, you can define it in file *makefile.common*. To build the jar file we are going to run in Hadoop, three hadoop jars are required. This jar files can be found inside libs folder. Depending on yout Hadoop installation you should use libs (for 64 bits) or libs32 (for 32 bits). Here follows an example of how to build **BigBWA**:

	git clone https://github.com/citiususc/BigBWA.git
	cd BigBWA
	make
		
This will generate *build* folder. Inside, we will find:

* **BigBWA.jar** - Jar file to launch in Hadoop.
* **bwa.zip** - File with the BWA library needed to run in Hadoop. It needs to be upload to the Hadoop distributed cache.

## Running BigBWA ##
To run **BigBWA** we need a working Hadoop cluster. Also, in this cluster, we need to ensure that we have available at least 9 GB of free memory per map, because each map is going to load into memory bwa index. We also need to take into account that **BigBWA** uses disk space in Hadoop tmp directory.

Here is an example of how to run **BigBWA** with BWA-MEM paired algorithm. This example assumes that our index is in all cluster nodes at */Data/HumanBase/* . The index can be obtained with bwa, using bwa index.

First we need to have input Fastq reads, we can get them from the 1000 Genomes project ftp.

	wget ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/data/NA12750/sequence_read/ERR000589_1.filt.fastq.gz
	wget ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/data/NA12750/sequence_read/ERR000589_2.filt.fastq.gz
	
Next we will uncompress these files:

	gzip -d ERR000589_1.filt.fastq.gz
	gzip -d ERR000589_2.filt.fastq.gz
	
After that, we need to use one of the Python tools in the utils folder:

	cd src/utils
	python Fq2FqBigDataPaired.py ../../ERR000589_1.filt.fastq ../../ERR000589_2.filt.fastq ../../ERR000589.fqBD
	cd ../../
	
Then, we have to upload input data into HDFS:

	hdfs dfs -copyFromLocal ERR000589.fqBD ERR000589.fqBD
	
And finally, we can run **BigBWA** in Hadoop:

	hadoop jar BigBWA.jar -archives bwa.zip -D mapreduce.input.fileinputformat.split.minsize=123641127 -D mapreduce.input.fileinputformat.split.maxsize=123641127 mem paired /Data/HumanBase/hg19 ERR000589.fqBD ExitERR000589
	
The syntax here is:
	hadoop jar BigBWA.jar -archives bwa.zip <hadoop_options> <aln|mem|memthread> <paired|single> [threads_number] <index_prefix> <in> <out>
	
After that, we will have the output in the HDFS. To get it to the local filesystem:

	mkdir Exit
	cd Exit 
	hdfs dfs -copyToLocal ExitERR000589/Output* ./
	cd ..
	
The output will be splited in pieces. If we want to ut it together we can use one of our Python utils or use samtools merge:

	cd src/utils
	python FullSam.py ../../Exit/ ../../Exit/OutputFile.sam
	cd ../../
	
If we used a reducer, the final output will be in one single file, so, the previous command will not be necessary. We can get the otput to the local filesystem by typing:
	
	hdfs dfs -copyToLocal ExitERR000589/part-r-00000 ./

##Frequently asked questions (FAQs)

1. [I can not build the tool because *jni_md.h* is missing.](#building1)


####<a name="building1"></a>1. I can not build the tool because *jni_md.h* or *jni.h* is missing.
You need to set correctly your *JAVA_HOME* environment variable. You can set it in Makefile.common if you don't have it in your environment:

	JAVA_HOME = /usr/lib/jvm/java


[1]: https://github.com/lh3/bwa
[2]: https://hadoop.apache.org/
[3]: http://en.wikipedia.org/wiki/FASTQ_format

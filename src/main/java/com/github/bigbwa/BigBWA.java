/**
 * Copyright 2016 José Manuel Abuín Mosquera <josemanuel.abuin@usc.es>
 * 
 * This file is part of BigBWA.
 *
 * BigBWA is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * BigBWA is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with BigBWA. If not, see <http://www.gnu.org/licenses/>.
 */

package com.github.bigbwa;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashMap;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class BigBWA extends Configured implements Tool {

	private static final Log LOG = LogFactory.getLog(BigBWA.class);

	public static void main(String[] args) throws Exception {

		int res = ToolRunner.run(new Configuration(), new BigBWA(), args);
		System.exit(res);

	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();

		
		for(String argumento: args){
			LOG.info("Arg: "+argumento);
		}

		String inputPath = "";
		String outputPath = "";

		boolean useReducer = false;

		BwaOptions options = new BwaOptions(args);
		
		//We set the timeout and set the bwa library to call BWA methods
		conf.set("mapreduce.task.timeout", "0");
		conf.set("mapreduce.map.env", "LD_LIBRARY_PATH=./bwa.zip/");


		//==================Algorithm election==================
		//One of the algorithms is going to be in use, because tge default is always specified.
		if (options.isMemAlgorithm()) {
			//Case of the mem algorithm
			conf.set("mem", "true");
			conf.set("aln", "false");
			conf.set("bwasw","false");
		}
		
		else if (options.isAlnAlgorithm()) {
			// Case of aln algorithm
			conf.set("mem", "false");
			conf.set("aln", "true");
			conf.set("bwasw","false");
		}
		
		else if (options.isBwaswAlgorithm()) {
			// Case of bwasw algorithm
			conf.set("mem", "false");
			conf.set("aln", "false");
			conf.set("bwasw","true");
		}

		//==================Index election==================
		if(!options.getIndexPath().equals("")){
			conf.set("indexRoute",options.getIndexPath());
		}
		else{
			System.err.println("No index has been found. Aborting.");
			System.exit(1);
		}
		
		//==================Type of reads election==================
		//There is always going to be a type of reads, because default is paired
		if(options.isPairedReads()){
			conf.set("paired", "true");
			conf.set("single", "false");
		}
		else if(options.isSingleReads()){
			conf.set("paired", "false");
			conf.set("single", "true");
		}
		
		//==================Use of reducer==================
		if(options.getUseReducer()){
			useReducer = true;
			conf.set("useReducer", "true");
		}
		else{
			conf.set("useReducer", "false");
		}

		//=================Number of threads and RG are changed by bwa options======================
		if(!options.getBwaArgs().isEmpty()) {
			conf.set("bwaArgs",options.getBwaArgs());
		}

		//==================Number of threads per map==================
		//if (options.getNumThreads() != "0"){
		//	conf.set("bwathreads", options.getNumThreads());
		//}
		
		//==================RG Header===================
		//if (options.getReadgroupHeader() != ""){
		//	conf.set("rgheader", options.getReadgroupHeader());
		//}
		
		
		//==================Input and output paths==================
		inputPath = options.getInputPath();
		outputPath = options.getOutputPath();

		conf.set("outputGenomics",outputPath);
		
		//==================Partition number==================
		if(options.getPartitionNumber() != 0) {
			try {
				FileSystem fs = FileSystem.get(conf);
				
				Path inputFilePath = new Path(inputPath);
				
				ContentSummary cSummary = fs.getContentSummary(inputFilePath);


				long length = cSummary.getLength();


				fs.close();
				
				conf.set("mapreduce.input.fileinputformat.split.maxsize", String.valueOf((length)/options.getPartitionNumber()));
				conf.set("mapreduce.input.fileinputformat.split.minsize", String.valueOf((length)/options.getPartitionNumber()));
			}
			catch (IOException e) {

				e.printStackTrace();
				LOG.error(e.toString());

				System.exit(1);
			}
			
		}
		
		
		//Job job = new Job(conf,"BigBWA_"+outputPath);
		Job job = Job.getInstance(conf,"BigBWA_"+outputPath);
		
		
		job.setJarByClass(BigBWA.class);
		job.setMapperClass(BigBWAMap.class);
		//job.setCombinerClass(BigBWACombiner.class);

		if(useReducer){
			job.setReducerClass(BigBWAReducer.class);

			job.setMapOutputKeyClass(IntWritable.class);
			job.setMapOutputValueClass(Text.class);

			job.setNumReduceTasks(1);
		}
		else{
			job.setNumReduceTasks(0);
		}

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);



		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		
		return(job.waitForCompletion(true) ? 0 : 1);
	}


	//Mapper class. We follow the In-Mapper Combining pattern
	public static class BigBWAMap extends Mapper<Object,Text,IntWritable,Text> {

		File fout;
		FileOutputStream fos;
		BufferedWriter bw;
		int identificador;
		
		String tmpFileString = "";
		int jobID;

		String tmpFileString2 = "";
		File fout2;
		
		//SAI files
		String saiFile1 = "";
		String saiFile2 = "";
		
		FileOutputStream fos2;
		BufferedWriter bw2;

		String[] initValues;
		String[] values1;
		String[] values2;

		String tmpDir;
		String indexRoute;

		String bwaArgs = "";
		
		String outputFileName = "";

		String outputDir = "";

		boolean memAlgorithm = false;
		boolean alnAlgorithm = false;
		boolean bwaswAlgorithm = false;

		boolean pairedReads = false;
		boolean singleReads = false;

		//In the setup, we create each split local file
		@Override
		protected void setup(Context context) {

			identificador = context.getTaskAttemptID().getTaskID().getId();
			jobID = context.getJobID().getId();

			Configuration conf = context.getConfiguration();

			tmpDir = conf.get("hadoop.tmp.dir","/tmp/");

			if((conf.get("mem")!=null)&&(conf.get("mem").equals("true"))) {
				this.memAlgorithm = true;
			}
			else if((conf.get("aln")!=null)&&(conf.get("aln").equals("true"))) {
				this.alnAlgorithm = true;
			}
			else if((conf.get("bwasw")!=null)&&(conf.get("bwasw").equals("true"))){
				this.bwaswAlgorithm = true;
			}

			this.outputDir = conf.get("outputGenomics");

			File tmpFile = new File(tmpDir);

			if(tmpDir == null || tmpDir.isEmpty() || !tmpFile.isDirectory() || !tmpFile.canWrite()) {
				tmpDir = "/tmp";
			}

			indexRoute = conf.get("indexRoute");

			tmpFileString = tmpDir+"/HadoopTMPFile-"+identificador+"-"+String.valueOf(jobID);

			fout = new File(tmpFileString);
			try {
				fos = new FileOutputStream(fout);
			} catch (FileNotFoundException e) {
				
				LOG.error(e.toString());
				e.printStackTrace();
			}

			bw = new BufferedWriter(new OutputStreamWriter(fos));


			if((conf.get("paired").equals("true"))){

				this.pairedReads = true;

				tmpFileString2 = tmpDir+"/HadoopTMPFile-"+identificador+"_2"+"-"+String.valueOf(jobID);
				fout2 = new File(tmpFileString2);

				try {
					fos2 = new FileOutputStream(fout2);
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				}

				bw2 = new BufferedWriter(new OutputStreamWriter(fos2));
			}
			else if((conf.get("single").equals("true"))){
				this.singleReads = true;
			}

			if((conf.get("bwaArgs")!=null) && (!conf.get("bwaArgs").equals(""))) {
				this.bwaArgs = conf.get("bwaArgs");
			}

			this.outputFileName = tmpDir+"/Output"+this.identificador+"-"+String.valueOf(jobID)+".sam";

		} 

		//In the map method, we write the FASTQ reads to the corresponding local files
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			try{

				//Configuration conf = context.getConfiguration();

				if(this.pairedReads){

					initValues = value.toString().split("<part>");

					values1 = initValues[0].split("<sep>");
					values2 = initValues[1].split("<sep>");

					for(String newValue: values1){
						bw.write(newValue);
						bw.newLine();
					}

					for(String newValue: values2){
						bw2.write(newValue);
						bw2.newLine();
					}

					values1=null;
					values2=null;
					initValues=null;

				}
				else{
					values1 = value.toString().split("<sep>");

					for(String newValue: values1){
						bw.write(newValue);
						bw.newLine();

					}
					values1=null;
				}


			}
			catch(Exception e){
				System.out.println(e.toString());
			}
		}

		//Finally, the computation and the calling to BWA methods, it is made in the cleanup method
		@Override
		public void cleanup(Context context) throws InterruptedException{

			try {

				Configuration conf = context.getConfiguration();

				//bw.flush();
				bw.close();

				if(this.pairedReads) {
					//bw2.flush();
					bw2.close();
				}

				this.run(0);

				//In case of the ALN algorithm, more executions of BWA are needed
				if (this.alnAlgorithm) {

					this.saiFile1 = tmpFileString + ".sai";
					//The next execution of BWA in the case of ALN algorithm
					this.run(1);

					//Finally, if we are talking about paired reads and aln algorithm, a final execution is needed
					if (this.pairedReads) {
						this.run(2);

						//Delete .sai file number 2
						this.saiFile2 = tmpFileString2 + ".sai";
						File tmpSaiFile2 = new File(tmpFileString2 + ".sai");
						tmpSaiFile2.delete();
					}

					//Delete *.sai file number 1
					File tmpSaiFile1 = new File(tmpFileString + ".sai");
					tmpSaiFile1.delete();
				}


				//We copy the results to HDFS and delete tmp files from local filesystem
				FileSystem fs = FileSystem.get(context.getConfiguration());

				fs.copyFromLocalFile(new Path(outputFileName), new Path(this.outputDir+"/Output"+this.identificador+".sam"));
				fs.copyFromLocalFile(new Path(tmpFileString), new Path(this.outputDir+"/Input"+this.identificador+"_1.fq"));

				if (this.pairedReads) {
					fs.copyFromLocalFile(new Path(tmpFileString2), new Path(outputDir+"/Input"+this.identificador+"_2.fq"));
					fout2.delete();
				}


				File outputFile = new File(outputFileName);
				outputFile.delete();

				fout.delete();


				if((conf.get("useReducer")!=null)&&(conf.get("useReducer").equals("true"))){
					context.write(new IntWritable(this.identificador), new Text(outputDir+"/Output"+this.identificador+".sam"));
				}




			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				
				//Clean temporary files
				
				//FASTQ splits
				this.fout.delete();
				this.fout2.delete();
				
				//SAI outputs
				if(!this.saiFile1.isEmpty()){
					File tempFile = new File(this.saiFile1);
					tempFile.delete();
				}
				
				if(!this.saiFile2.isEmpty()){
					File tempFile = new File(this.saiFile2);
					tempFile.delete();
				}

				//SAM Output
				if(!this.outputFileName.isEmpty()){
					File tempFile = new File(this.outputFileName);
					tempFile.delete();
				}
				

				
			}

		}

		/**
		 *
		 * @param alnStep Param to know if the aln algorithm is going to be used and BWA functions need to be executes more than once
		 * @return A String array containing the parameters to launch BWA
		 */
		private String[] parseParameters(int alnStep) {
			ArrayList<String> parameters = new ArrayList<String>();

			//The first parameter is always "bwa"======================================================
			parameters.add("bwa");

			//The second parameter is the algorithm election===========================================
			String algorithm = "";

			//Case of "mem" algorithm
			if (this.memAlgorithm && !this.alnAlgorithm && !this.bwaswAlgorithm) {
				algorithm = "mem";
			}
			//Case of "aln" algorithm
			else if (!this.memAlgorithm && this.alnAlgorithm && !this.bwaswAlgorithm) {
				//Aln algorithm and paired reads
				if (this.pairedReads) {
					//In the two first steps, the aln option is used
					if (alnStep < 2) {
						algorithm = "aln";
					}
					//In the third one, the "sampe" has to be performed
					else {
						algorithm = "sampe";
					}
				}
				//Aln algorithm single reads
				else if (this.singleReads) {
					//In the first step the "aln" ins performed
					if (alnStep == 0) {
						algorithm = "aln";
					}
					//In the second step, the "samse" is performed
					else {
						algorithm = "samse";
					}
				}
			}

			//The last case is the "bwasw"
			else if (!this.memAlgorithm && !this.alnAlgorithm && this.bwaswAlgorithm) {
				algorithm = "bwasw";
			}

			parameters.add(algorithm);

			// If extra BWA parameters are added
			if (!this.bwaArgs.isEmpty()) {

				String[] arrayBwaArgs = this.bwaArgs.split(" ");
				int numBwaArgs = arrayBwaArgs.length;

				for( int i = 0; i< numBwaArgs; i++) {
					parameters.add(arrayBwaArgs[i]);
				}

				//parameters.add(this.bwaArgs);
			}

			//The third parameter is the output file===================================================
			parameters.add("-f");

			if (algorithm.equals("aln")) {
				if (alnStep == 0) {
					parameters.add(this.tmpFileString + ".sai");
				}
				else if (alnStep == 1 && this.pairedReads) {
					parameters.add(this.tmpFileString2 + ".sai");
				}
			}
			else {
				// For all other algorithms the output is a SAM file.
				parameters.add(this.outputFileName);
			}

			//The fifth, the index path===============================================================
			parameters.add(this.indexRoute);

			//The sixth, the input files===============================================================

			//If the "mem" algorithm, we add the FASTQ files
			if (algorithm.equals("mem") || algorithm.equals("bwasw")) {
				parameters.add(this.tmpFileString);

				if (this.pairedReads) {
					parameters.add(this.tmpFileString2);
				}
			}

			//If "aln" algorithm, and aln step is 0 or 1, also FASTQ files
			else if (algorithm.equals("aln")) {
				if (alnStep == 0) {
					parameters.add(this.tmpFileString);
				}
				else if (alnStep == 1 && this.pairedReads) {
					parameters.add(this.tmpFileString2);
				}
			}

			//If "sampe" the input files are the .sai from previous steps
			else if (algorithm.equals("sampe")) {
				parameters.add(this.tmpFileString + ".sai");
				parameters.add(this.tmpFileString2 + ".sai");
				parameters.add(this.tmpFileString);
				parameters.add(this.tmpFileString2);
			}

			//If "samse", only one .sai file
			else if (algorithm.equals("samse")) {
				parameters.add(this.tmpFileString + ".sai");
				parameters.add(this.tmpFileString);
			}

			String[] parametersArray = new String[parameters.size()];

			return parameters.toArray(parametersArray);
		}

		/**
		 * This Function is responsible for creating the options that are going to be passed to BWA
		 *
		 * @param alnStep An integer that indicates at with phase of the aln step the program is
		 * @return A Strings array containing the options which BWA was launched
		 */
		public int run(int alnStep) {
			// Get the list of arguments passed by the user
			String[] parametersArray = parseParameters(alnStep);

			// Call to JNI with the selected parameters
			int returnCode = BwaJni.Bwa_Jni(parametersArray);

			if (returnCode != 0) {
				LOG.error("["+this.getClass().getName()+"] :: BWA exited with error code: " + String.valueOf(returnCode));
				return returnCode;
			}

			// The run was successful
			return 0;
		}

	}

	public static class BigBWAReducer extends Reducer<IntWritable,Text,NullWritable,Text> {

		//private String outputFile;
		//private String outputDir;
		private HashMap<Integer,String> inputFiles;

		@Override
		protected void setup(Context context) {

			//this.outputDir = context.getConfiguration().get("outputGenomics");
			//this.outputFile = this.outputDir+"/FinalOutput.sam";

			this.inputFiles = new HashMap<Integer,String>();

		}

		@Override
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			try{

				//In theory, there is just one value per key
				for (Text val : values) {
					inputFiles.put(key.get(), val.toString());
				}

			}
			catch(Exception e){
				System.out.println(e.toString());
			}
		}

		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {

			FileSystem fs = FileSystem.get(context.getConfiguration());

			int fileNumber = this.inputFiles.size();

			boolean readHeader = true;

			for(int i = 0; i< fileNumber; i++){

				String currentFile = this.inputFiles.get(i);

				BufferedReader d = new BufferedReader(new InputStreamReader(fs.open(new Path(currentFile))));

				String line = "";

				while ((line = d.readLine())!=null) {

					if((line.startsWith("@") && readHeader) || (!line.startsWith("@")) ){
						//bufferOut.write(line);
						context.write(NullWritable.get(), new Text(line));
					}


				}

				readHeader = false;

				d.close();

				fs.delete(new Path(currentFile), true);

			}

			//bufferOut.close();

		}

	}

}


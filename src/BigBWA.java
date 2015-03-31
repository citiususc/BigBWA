/**
 * Copyright 2015 José Manuel Abuín Mosquera <josemanuel.abuin@usc.es>
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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.HashMap;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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
		// TODO Auto-generated method stub
		int res = ToolRunner.run(new Configuration(), new BigBWA(), args);
		System.exit(res);

	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();

		String correctUse = "hadoop jar BigBWA.jar -archives bwa.zip <hadoop_options> <-aln|-mem|-memthread> [threads_number] <-paired|-single> <-index index_prefix> <in> <out>";

		/*if (args.length < 3) {
			System.err.println("Usage: hadoop jar BigBWA.jar -archives bwa.zip <hadoop_options> <aln|mem|memthread> <paired|single> [threads_number] <index_prefix> <in> <out> ");
			System.exit(2);
		}*/
		for(String argumento: args){
			System.out.println("Arg: "+argumento);
		}

		String inputPath = "";
		String outputPath = "";

		boolean useReducer = false;

		//We set the timeout and stablish the bwa library to call BWA methods
		conf.set("mapreduce.task.timeout", "0");
		conf.set("mapreduce.map.env", "LD_LIBRARY_PATH=./bwa.zip/");



		//Parse arguments
		Options options = new Options();

		//Algorithm options
		options.addOption("mem", false, "Enables mem algorithm");
		options.addOption("aln", false, "Enables aln algorithm");
		options.addOption("bwasw", false, "Enables bwasw algorithm");

		Option memthread   = OptionBuilder.withArgName( "Threads number" )
				.hasArg()
				.withDescription(  "Number of threads used per map" )
				.create( "memthread" );


		options.addOption(memthread);

		//Paired or single
		options.addOption("paired", false, "Enables mem hybrid algorithm");
		options.addOption("single", false, "Enables mem hybrid algorithm");

		//Reducer option
		options.addOption("r",false,"Enables the reducer");

		//Index
		Option index   = OptionBuilder.withArgName( "Index prefix" )
				.hasArg()
				.withDescription(  "Prefix for the index created by bwa to use." )
				.create( "index" );


		options.addOption(index);

		//To print the help
		HelpFormatter formatter = new HelpFormatter();


		//Parse the given arguments
		CommandLineParser parser = new BasicParser();
		CommandLine cmd = parser.parse( options, args);


		//We look for the algorithm
		if((cmd.hasOption("mem")) && (!cmd.hasOption("aln"))&&(!cmd.hasOption("memthread")) && (!cmd.hasOption("bwasw"))) {
			//Case of the mem algorithm
			conf.set("mem", "true");
			conf.set("aln", "false");
			conf.set("bwasw","false");

		}
		else if ((!cmd.hasOption("mem")) && (cmd.hasOption("aln"))&&(!cmd.hasOption("memthread")) && (!cmd.hasOption("bwasw"))){
			// Case of aln algorithm
			conf.set("mem", "false");
			conf.set("aln", "true");
			conf.set("bwasw","false");
		}
		else if ((!cmd.hasOption("mem")) && (!cmd.hasOption("aln"))&&(cmd.hasOption("memthread")) && (!cmd.hasOption("bwasw"))){
			// Case of mem hybrid algorithm
			conf.set("mem", "false");
			conf.set("aln", "true");
			conf.set("bwasw","false");

			//We need to get the number of threads per map
			conf.set("bwathreads", cmd.getOptionValue("memthread"));
		}
		else if ((!cmd.hasOption("mem")) && (!cmd.hasOption("aln"))&&(!cmd.hasOption("memthread")) && (cmd.hasOption("bwasw"))){
			// Case of bwasw algorithm
			conf.set("mem", "false");
			conf.set("aln", "false");
			conf.set("bwasw","true");
		}
		else{ //No algorithm present, abort.
			System.err.println("No algorithm has been found. Aborting.");
			formatter.printHelp( correctUse, options );
			System.exit(2);
		}


		//We look for the index
		if(cmd.hasOption("index")){
			conf.set("indexRoute",cmd.getOptionValue("index"));
		}
		else{
			System.err.println("No index has been found. Aborting.");
			formatter.printHelp( correctUse, options );
			System.exit(2);
		}

		//We look if we want the paired or single algorithm
		if((cmd.hasOption("paired"))&&(!cmd.hasOption("single"))){
			conf.set("paired", "true");
			conf.set("single", "false");
		}
		else if((cmd.hasOption("single"))&&(!cmd.hasOption("paired"))){
			conf.set("paired", "false");
			conf.set("single", "true");
		}
		else{
			System.err.println("No paired or single has been found. Aborting.");
			formatter.printHelp( correctUse, options );
			System.exit(2);
		}

		//We look if the user wants to use a reducer or not
		if(cmd.hasOption("r")){
			useReducer = true;
			conf.set("useReducer", "true");
		}
		else{
			conf.set("useReducer", "false");
		}

		//Input and output paths
		String otherArguments[] = cmd.getArgs(); //With this we get the rest of the arguments

		if(otherArguments.length != 2){
			System.err.println("No input and output has been found. Aborting.");
			formatter.printHelp( correctUse, options );
			System.exit(2);
		}

		inputPath = otherArguments[0];
		outputPath = otherArguments[1];

		conf.set("outputGenomics",outputPath);


		Job job = new Job(conf,"BigBWA_"+outputPath);



		job.setJarByClass(BigBWA.class);
		job.setMapperClass(BigBWAMap.class);
		//job.setCombinerClass(BigBWACombiner.class);

		if(useReducer){
			job.setReducerClass(BigBWAReducer.class);

			job.setMapOutputKeyClass(IntWritable.class);
			job.setMapOutputValueClass(Text.class);

			job.setOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(Text.class);

			job.setNumReduceTasks(1);
		}



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
		String tmpFileString;
		int jobID;

		String tmpFileString2;
		File fout2;
		FileOutputStream fos2;
		BufferedWriter bw2;

		String[] initValues;
		String[] values1;
		String[] values2;

		String tmpDir;
		String indexRoute;

		//In the setup, we create each split local file
		@Override
		protected void setup(Context context) {

			identificador = context.getTaskAttemptID().getTaskID().getId();
			jobID = context.getJobID().getId();

			Configuration conf = context.getConfiguration();

			tmpDir = conf.get("hadoop.tmp.dir");
			indexRoute = conf.get("indexRoute");

			tmpFileString = tmpDir+"/HadoopTMPFile-"+identificador+"-"+String.valueOf(jobID);

			fout = new File(tmpFileString);
			try {
				fos = new FileOutputStream(fout);
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			bw = new BufferedWriter(new OutputStreamWriter(fos));


			if((conf.get("paired").equals("true"))){
				tmpFileString2 = tmpDir+"/HadoopTMPFile-"+identificador+"_2"+"-"+String.valueOf(jobID);
				fout2 = new File(tmpFileString2);

				try {
					fos2 = new FileOutputStream(fout2);
				} catch (FileNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

				bw2 = new BufferedWriter(new OutputStreamWriter(fos2));
			}

		} 

		//in the map method, we write the fastq reads to the corresponding local files
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			try{

				Configuration conf = context.getConfiguration();

				if((conf.get("paired").equals("true"))){

					initValues = value.toString().split("<part>");

					values1 = initValues[0].toString().split("<sep>");
					values2 = initValues[1].toString().split("<sep>");

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
				String[] args;
				String outputFileName = "";

				bw.close();

				String outputDir = context.getConfiguration().get("outputGenomics");
				
				//Paired algorithms
				if((conf.get("paired").equals("true"))){
					bw2.close();

					

					if(conf.get("bwathreads")!=null && conf.get("bwathreads").equals("")){
						args = new String[9];

						args[0] = "bwa";
						args[1] = "mem";
						args[2] = "-f";
						args[3] = tmpDir+"/Output"+this.identificador+"-"+String.valueOf(jobID)+".sam";
						args[4] = "-t";
						args[5] = conf.get("bwathreads");
						args[6] = indexRoute;
						args[7] = tmpFileString;
						args[8] = tmpFileString2;

						outputFileName = args[3];

						//bwa execution
						BwaJni.Bwa_Jni(args);
					}
					else if((conf.get("mem")!=null)&&(conf.get("mem").equals("true"))){
						args = new String[7];

						args[0] = "bwa";
						args[1] = "mem";
						args[2] = "-f";
						args[3] = tmpDir+"/Output"+this.identificador+"-"+String.valueOf(jobID)+".sam";
						args[4] = indexRoute;
						args[5] = tmpFileString;
						args[6] = tmpFileString2;

						outputFileName = args[3];

						//bwa execution
						BwaJni.Bwa_Jni(args);
					}
					else if((conf.get("bwasw")!=null)&&(conf.get("bwasw").equals("true"))){
						args = new String[7];

						args[0] = "bwa";
						args[1] = "bwasw";
						args[2] = "-f";
						args[3] = tmpDir+"/Output"+this.identificador+"-"+String.valueOf(jobID)+".sam";
						args[4] = indexRoute;
						args[5] = tmpFileString;
						args[6] = tmpFileString2;

						outputFileName = args[3];

						//bwa execution
						BwaJni.Bwa_Jni(args);
					}
					else if((conf.get("aln")!=null)&&(conf.get("aln").equals("true"))){
						args = new String[6];

						String saiFile1 = tmpDir+"/Output"+this.identificador+"-"+String.valueOf(jobID)+".sai";
						String saiFile2 = tmpDir+"/Output"+this.identificador+"-2-"+String.valueOf(jobID)+".sai";

						args[0] = "bwa";
						args[1] = "aln";
						args[2] = "-f";
						args[3] = saiFile1;
						args[4] = indexRoute;
						args[5] = tmpFileString;

						//bwa execution for aln1
						BwaJni.Bwa_Jni(args);

						LOG.warn("End of first alignment");
						String[] args2 = new String[6];

						args2[0] = "bwa";
						args2[1] = "aln";
						args2[2] = "-f";
						args2[3] = saiFile2;
						args2[4] = indexRoute;
						args2[5] = tmpFileString2;

						LOG.warn("begin of second alignment");
						for(String newArg: args2){
							LOG.warn("Arg: "+newArg);
						}

						//bwa execution for aln2
						BwaJni.Bwa_Jni(args2);

						args = new String[9];
						args[0] = "bwa";
						args[1] = "sampe";
						args[2] = "-f";
						args[3] = tmpDir+"/Output"+this.identificador+"-"+String.valueOf(jobID)+".sam";
						args[4] = indexRoute;
						args[5] = saiFile1;
						args[6] = saiFile2;
						args[7] = tmpFileString;
						args[8] = tmpFileString2;

						//bwa execution of sampe
						BwaJni.Bwa_Jni(args);

						File tempFile = new File(saiFile1);
						tempFile.delete();

						tempFile = new File(saiFile2);
						tempFile.delete();

						outputFileName = args[3];

					}

					//We copy the results to HDFS and delete tmp files from local filesystem
					FileSystem fs = FileSystem.get(context.getConfiguration());

					fs.copyFromLocalFile(new Path(outputFileName), new Path(outputDir+"/Output"+this.identificador+".sam"));
					fs.copyFromLocalFile(new Path(tmpFileString), new Path(outputDir+"/Input"+this.identificador+"_1.fq"));
					fs.copyFromLocalFile(new Path(tmpFileString2), new Path(outputDir+"/Input"+this.identificador+"_2.fq"));

					File outputFile = new File(outputFileName);
					outputFile.delete();

					fout.delete();
					fout2.delete();

					if((conf.get("useReducer")!=null)&&(conf.get("useReducer").equals("true"))){
						context.write(new IntWritable(this.identificador), new Text(outputDir+"/Output"+this.identificador+".sam"));
					}


				}
				//Single algorithms
				else{
					if(conf.get("mem").equals("true")){
						//String outputDir = context.getConfiguration().get("outputGenomics");
						args = new String[6];

						args[0] = "bwa";
						args[1] = "mem";
						args[2] = "-f";
						args[3] = tmpDir+"/Output"+this.identificador+"-"+String.valueOf(jobID)+".sam";
						args[4] = indexRoute;
						args[5] = tmpFileString;

						//bwa execution
						BwaJni.Bwa_Jni(args);

						//We copy the results to HDFS and delete tmp files from local filesystem
						FileSystem fs = FileSystem.get(context.getConfiguration());

						fs.copyFromLocalFile(new Path(args[3]), new Path(outputDir+"/Output"+this.identificador+".sam"));
						fs.copyFromLocalFile(new Path(tmpFileString), new Path(outputDir+"/Input"+this.identificador+".fq"));

						File outputFile = new File(args[3]);
						outputFile.delete();
						fout.delete();

					}
					else if(conf.get("bwasw").equals("true")){
						//String outputDir = context.getConfiguration().get("outputGenomics");
						args = new String[6];

						args[0] = "bwa";
						args[1] = "bwasw";
						args[2] = "-f";
						args[3] = tmpDir+"/Output"+this.identificador+"-"+String.valueOf(jobID)+".sam";
						args[4] = indexRoute;
						args[5] = tmpFileString;

						//bwa execution
						BwaJni.Bwa_Jni(args);

						//We copy the results to HDFS and delete tmp files from local filesystem
						FileSystem fs = FileSystem.get(context.getConfiguration());

						fs.copyFromLocalFile(new Path(args[3]), new Path(outputDir+"/Output"+this.identificador+".sam"));
						fs.copyFromLocalFile(new Path(tmpFileString), new Path(outputDir+"/Input"+this.identificador+".fq"));

						File outputFile = new File(args[3]);
						outputFile.delete();
						fout.delete();

					}
					else if(conf.get("aln").equals("true")){
						args = new String[6];
						
						String saiFile = tmpDir+"/Output"+this.identificador+"-"+String.valueOf(jobID)+".sai";

						args[0] = "bwa";
						args[1] = "aln";
						args[2] = "-f";
						args[3] = saiFile;
						args[4] = indexRoute;
						args[5] = tmpFileString;

						//bwa execution
						BwaJni.Bwa_Jni(args);

						args = new String[7];
						args[0] = "bwa";
						args[1] = "samse";
						args[2] = "-f";
						args[3] = tmpDir+"/Output"+this.identificador+"-"+String.valueOf(jobID)+".sam";
						args[4] = indexRoute;
						args[5] = saiFile;
						args[6] = tmpFileString;


						//bwa execution of sampe
						BwaJni.Bwa_Jni(args);

						File tempFile = new File(saiFile);
						tempFile.delete();

						outputFileName = args[3];
						
						//We copy the results to HDFS and delete tmp files from local filesystem
						//String outputDir = context.getConfiguration().get("outputGenomics");

						FileSystem fs = FileSystem.get(context.getConfiguration());

						fs.copyFromLocalFile(new Path(args[3]), new Path(outputDir+"/Output"+this.identificador+".sai"));
						fs.copyFromLocalFile(new Path(tmpFileString), new Path(outputDir+"/Input"+this.identificador+".fq"));

						File outputFile = new File(args[3]);

						fout.delete();
						outputFile.delete();
					}
					
					if((conf.get("useReducer")!=null)&&(conf.get("useReducer").equals("true"))){
						context.write(new IntWritable(this.identificador), new Text(outputDir+"/Output"+this.identificador+".sam"));
					}

				}

			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}


	}

	public static class BigBWAReducer extends Reducer<IntWritable,Text,NullWritable,Text> {

		private String outputFile;
		private String outputDir;
		private HashMap<Integer,String> inputFiles;

		@Override
		protected void setup(Context context) {

			this.outputDir = context.getConfiguration().get("outputGenomics");
			this.outputFile = this.outputDir+"/FinalOutput.sam";

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


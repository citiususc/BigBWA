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

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
//import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.UnrecognizedOptionException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class BwaOptions {

	//Option to use the reduce phase
	private boolean useReducer = false;


	//Algorithms boolean variables
	private boolean memAlgorithm 	= true;
	private boolean alnAlgorithm 	= false;
	private boolean bwaswAlgorithm 	= false;

	//private boolean memThread 		= false;
	private String numThreads 		= "0";

	//Paired or single reads
	private boolean pairedReads 	= true;
	private boolean singleReads 	= false;

	//Index path
	private String indexPath 		= "";

	private String inputFile 		= "";
	private String inputFile2 		= "";
	private String outputFile 		= "";

	private String outputHdfsDir	= "";

	private String inputPath		= "";
	private String inputPath2		= "";

	private boolean sortFastqReads		= false;
	private boolean sortFastqReadsHdfs	= false;


	private String correctUse 			= "hadoop jar BigBWA.jar -archives bwa.zip <hadoop_options> [-algorithm <mem|aln|bwasw>] [-threads <threads_number>] [-reads <paired|single>] -index >index_prefix> <in> <out>\n"
										+ "\n\n"
										//+ "To set the Input.fastq - setInputPath(string)\n"
										//+ "To set the Input2.fastq - setInputPath2(string)\n"
										//+ "To set the Output - setOutputPath(string)\n"
										+ "The available BigBWA options are: \n\n";
	
	private String header 				= "Performs genomic alignment using bwa in a Hadoop cluster\n\n";
	private String footer 				= "\nPlease report issues at josemanuel.abuin@usc.es";


	private String outputPath		= "";

	private int partitionNumber		= 0;



	private static final Log LOG = LogFactory.getLog(BwaOptions.class);

	public BwaOptions(){

		Options options = this.initOptions();
		
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp( correctUse,header, options,footer , true);
		
	}


	public BwaOptions(String [] args){

		//Parse arguments
		for(String argumento: args){
			LOG.info("Received argument: "+argumento);
		}


		//Algorithm options

		Options options = this.initOptions();

		//To print the help
		
		HelpFormatter formatter = new HelpFormatter();
		//formatter.printHelp( correctUse,header, options,footer , true);

		//Parse the given arguments
		CommandLineParser parser = new BasicParser();
		CommandLine cmd;
		try {
			cmd = parser.parse( options, args);


			//Number of threads per map task
			if(cmd.hasOption("threads")){
				numThreads = cmd.getOptionValue("threads");
			}
			
			//We look for the algorithm
			//if((cmd.hasOption("mem")) && (!cmd.hasOption("aln")) && (!cmd.hasOption("bwasw"))) {
			if (cmd.hasOption("algorithm")) {
				
				
				if(cmd.getOptionValue("algorithm").equals("mem")){
					//Case of the mem algorithm
					memAlgorithm = true;
					alnAlgorithm = false;
					bwaswAlgorithm = false;
				}
				
				else if(cmd.getOptionValue("algorithm").equals("aln")) {
					// Case of aln algorithm
					alnAlgorithm = true;
					memAlgorithm = false;
					bwaswAlgorithm = false;
				}
				
				else if(cmd.getOptionValue("algorithm").equals("bwasw")) {
					// Case of bwasw algorithm
					bwaswAlgorithm = true;
					memAlgorithm = false;
					alnAlgorithm = false;
				}
				
				else{
					LOG.warn("The algorithm "+cmd.getOptionValue("algorithm")+" could not be found\nSetting to default mem algorithm\n");
					memAlgorithm = true;
					alnAlgorithm = false;
					bwaswAlgorithm = false;
					
				}

			}


			//We look for the index
			if(cmd.hasOption("index")){
				indexPath = cmd.getOptionValue("index");
			}
			else{
				System.err.println("No index has been found. Aborting.");
				formatter.printHelp( correctUse,header, options,footer , true);
				System.exit(1);
			}

			//Partition number
			if(cmd.hasOption("partitions")){
				partitionNumber = Integer.parseInt(cmd.getOptionValue("partitions"));
			}

			//We look if we want the paired or single algorithm
			if(cmd.hasOption("reads")) {
				if(cmd.getOptionValue("reads").equals("single")) {
					pairedReads = false;
					singleReads = true;
				}
				
				else if (cmd.getOptionValue("reads").equals("paired")) {
					pairedReads = true;
					singleReads = false;
				}
				
				else {
					LOG.warn("Reads argument could not be found\nSetting it to default paired reads\n");
					pairedReads = true;
					singleReads = false;
				}
			}
			
			

			//We look if the user wants to use a reducer or not
			if(cmd.hasOption("r")){
				useReducer = true;
			}
			else{
				useReducer = false;
			}

			//Input and output paths
			String otherArguments[] = cmd.getArgs(); //With this we get the rest of the arguments

			if((otherArguments.length != 2) && (otherArguments.length != 3)){
				LOG.error("No input and output has been found. Aborting.");

				for(String tmpString: otherArguments){
					LOG.error("Other args:: "+tmpString);
				}

				formatter.printHelp( correctUse,header, options,footer , true);
				System.exit(1);
			}

			else if(otherArguments.length == 2){
				inputPath = otherArguments[0];
				outputPath = otherArguments[1];
			}
			else if (otherArguments.length == 3){
				inputPath = otherArguments[0];
				inputPath2 = otherArguments[1];
				outputPath = otherArguments[2];
			}


		} catch (UnrecognizedOptionException e){
			e.printStackTrace();
			formatter.printHelp( correctUse,header, options,footer , true);
			System.exit(1);
			
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			//formatter.printHelp( correctUse,header, options,footer , true);
			e.printStackTrace();
			System.exit(1);
		}

	}

	
	public Options initOptions(){
		

		Options options = new Options();

		//Algorithm options
		Option algorithm = new Option("algorithm",true,"Specify the algorithm to use during the alignment");
		algorithm.setArgName("Algorithm to use during alignment");
		
		options.addOption(algorithm);
		
		//Number of threads
		Option threads = new Option("threads",true,"Number of threads used per map - setNumThreads(string)");
		threads.setArgName("Threads number");

		options.addOption(threads);

		//Paired or single reads
		Option reads = new Option("reads",true,"Type of reads to use during alignment");
		reads.setArgName("Type of reads");
		
		options.addOption(reads);

		//Reducer option
		Option reducer = new Option("r",false,"Enables the reducer phase - setUseReducer(boolean)");
		options.addOption(reducer);
		

		//Index
		Option index = new Option("index",true,"Prefix for the index created by bwa to use - setIndexPath(string)");
		index.setArgName("Index prefix");


		options.addOption(index);

		//Partition number
		Option partitions = new Option("partitions", true, "Number of partitions to divide input reads - setPartitionNumber(int)");
		partitions.setArgName("Number of partitions");


		options.addOption(partitions);

		return options;
	}


	public void setOutputHdfsDir(String outputHdfsDir) {
		this.outputHdfsDir = outputHdfsDir;
	}

	public String getInputFile2() {
		return inputFile2;
	}

	public void setInputFile2(String inputFile2) {
		this.inputFile2 = inputFile2;
	}

	public String getInputFile() {
		return inputFile;
	}

	public void setInputFile(String inputFile) {
		this.inputFile = inputFile;
	}

	public String getOutputFile() {
		return outputFile;
	}

	public void setOutputFile(String outputFile) {
		this.outputFile = outputFile;
	}

	public boolean isUseReducer() {
		return useReducer;
	}

	public void setUseReducer(boolean useReducer) {
		this.useReducer = useReducer;
	}

	public boolean isMemAlgorithm() {
		return memAlgorithm;
	}

	public void setMemAlgorithm(boolean memAlgorithm) {
		this.memAlgorithm = memAlgorithm;
		
		if(memAlgorithm){
			this.setAlnAlgorithm(false);
			this.setBwaswAlgorithm(false);
		}
		
	}

	public boolean isAlnAlgorithm() {
		return alnAlgorithm;
	}

	public void setAlnAlgorithm(boolean alnAlgorithm) {
		this.alnAlgorithm = alnAlgorithm;
		
		if(alnAlgorithm){
			this.setMemAlgorithm(false);
			this.setBwaswAlgorithm(false);
		}
	}

	public boolean isBwaswAlgorithm() {
		return bwaswAlgorithm;
	}

	public void setBwaswAlgorithm(boolean bwaswAlgorithm) {
		this.bwaswAlgorithm = bwaswAlgorithm;
		
		if(bwaswAlgorithm){
			this.setAlnAlgorithm(false);
			this.setMemAlgorithm(false);
		}
	}

	public String getNumThreads() {
		return numThreads;
	}

	public void setNumThreads(String numThreads) {
		this.numThreads = numThreads;
	}

	public boolean isPairedReads() {
		return pairedReads;
	}

	public void setPairedReads(boolean pairedReads) {
		this.pairedReads = pairedReads;
	}

	public boolean isSingleReads() {
		return singleReads;
	}

	public void setSingleReads(boolean singleReads) {
		this.singleReads = singleReads;
	}

	public String getIndexPath() {
		return indexPath;
	}

	public void setIndexPath(String indexPath) {
		this.indexPath = indexPath;
	}

	public String getOutputHdfsDir() {
		return outputHdfsDir;
	}

	public String getInputPath() {
		return inputPath;
	}


	public void setInputPath(String inputPath) {
		this.inputPath = inputPath;
	}


	public String getOutputPath() {
		return outputPath;
	}


	public void setOutputPath(String outputPath) {
		this.outputPath = outputPath;
	}

	public int getPartitionNumber() {
		return partitionNumber;
	}


	public void setPartitionNumber(int partitionNumber) {
		this.partitionNumber = partitionNumber;
	}

	public String getInputPath2() {
		return inputPath2;
	}


	public void setInputPath2(String inputPath2) {
		this.inputPath2 = inputPath2;
	}

	public boolean isSortFastqReads() {
		return sortFastqReads;
	}


	public void setSortFastqReads(boolean sortFastqReads) {
		this.sortFastqReads = sortFastqReads;
	}

	public boolean isSortFastqReadsHdfs() {
		return sortFastqReadsHdfs;
	}


	public void setSortFastqReadsHdfs(boolean sortFastqReadsHdfs) {
		this.sortFastqReadsHdfs = sortFastqReadsHdfs;
	}
}




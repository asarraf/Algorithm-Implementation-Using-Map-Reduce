// Till now : I am able to find the ranks of the out links from each node in 10 iterations
// What now?
// Read the total inLink Ranks
// Make the decimal point up to 8 digits after decimal <Optional>

package pagerank;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import filepreprocess.HadoopDFSFileReadWrite;
import finalpagerank.FinalPageRankCalculator;

public class PageRank {
	private static final transient Logger LOG = LoggerFactory.getLogger(PageRank.class);
	private static volatile int roundNumber = 0;

	// Consider there are only 5 nodes
	public static final int TOTALNODES = 5;

	// Counter
	public enum MyCounter {
		COUNTER;
	}

	// Some Pre - Processing of the Input file
	static {
		HadoopDFSFileReadWrite preprocessor = new HadoopDFSFileReadWrite();
		String originalInputFile = "/pageRank/input/originalinput.txt";
		String newInputFile = "/pageRank/input/pagerankinput.txt";
		try{
			preprocessor.preprocess(originalInputFile, newInputFile);
		} catch(Exception e) {
			LOG.info("Some Error In Reading the Input File");
			LOG.info(e.getMessage());
			System.exit(0);
		} finally {
			LOG.info("No Error In Reading the Input File");
			// Proceed to the Map Reduce Job
		}
	}

	public static void main(String[] args) throws Exception {
		String inputPath = "/pageRank/input/pagerankinput.txt";
		String outputPath = "/pageRank/outputs/output";
		String finalPath = "/pageRank/finalOutput/finalOutput.txt";
		Counter counter;

		do {
			Configuration conf = new Configuration();
			Job job = Job.getInstance(conf);

			deleteFolder(conf, outputPath + roundNumber);

			LOG.info("Input : " + inputPath + " :: Output : " + outputPath);

			myMapReduceTask(job, inputPath, outputPath + roundNumber);
			inputPath = outputPath + roundNumber + "/part-r-00000";
			roundNumber++;

			// Configure the Counter
			counter = job.getCounters().findCounter(MyCounter.COUNTER);

			LOG.info("Counter Value : " + counter.getValue());
		} while(counter.getValue() > 0);
		// The above loop executes til the time the Page ranks Stabilize
		
		// Now calculate the sum of In Links for each node
		FinalPageRankCalculator finalPageRankCalculator = new FinalPageRankCalculator();
		finalPageRankCalculator.getFinalPageRank(
				outputPath + (roundNumber - 1) + "/part-r-00000", finalPath);
		
		LOG.info("Final Page Rank File Created");
		LOG.info("Check the Final Output in the path /pageRank/finalOutput/finalOutput.txt");
	}

	private static void myMapReduceTask(Job job, String inputPath, String outputPath) 
			throws IOException, ClassNotFoundException, InterruptedException {
		job.setJarByClass(PageRank.class);

		// Set the Mapper Class
		job.setMapperClass(PageRankMapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);

		// Set the Reducer Class
		job.setReducerClass(PageRankReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		// Specify input and output Directories
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		// Condition to wait for the completion of MR Job

		while(!job.waitForCompletion(true)) {}

		return;
	}

	private static void deleteFolder(Configuration conf, String folderPath ) throws IOException {
		// Delete the Folder
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(folderPath);
		if(fs.exists(path)) {
			fs.delete(path,true);
		}
	}
}
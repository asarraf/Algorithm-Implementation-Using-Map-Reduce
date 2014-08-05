package invertedindex;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import constants.Constants;

public class InvertedIndex {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "JobName");
		job.setJarByClass(invertedindex.InvertedIndex.class);

		// Code can be inserted to do the Data Munging
		// That can include removing stop words, punctuation, etc.
		// But that is not the scope of this Project

		// Delete Output directory if it already exists
		deleteFolder(conf, Constants.outputPath);

		myMapReduceTask(job, Constants.inputPath, Constants.outputPath);
	}

	public static void deleteFolder(Configuration conf, String folderPath)
			throws IOException{
		// Delete the Folder
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(folderPath);
		if(fs.exists(path)) {
			fs.delete(path,true);
		}
	}

	public static void myMapReduceTask(Job job, String inputPath, String outputPath) throws
	IllegalArgumentException,
	IOException,
	ClassNotFoundException,
	InterruptedException {
		// Set Mapper Class
		job.setMapperClass(InvertedIndexMapper.class);

		// Set Mapper Output Types
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		// Set Reducer Class
		job.setReducerClass(InvertedIndexReducer.class);

		// Set the Reducer Output Types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// Specify input and output Directories
		FileInputFormat.addInputPath(job, new Path(Constants.inputPath));
		FileOutputFormat.setOutputPath(job, new Path(Constants.outputPath));

		// Wait condition for the Mapper and Reducer Class to finish their execution
		if (!job.waitForCompletion(true))
			return;
	}
}
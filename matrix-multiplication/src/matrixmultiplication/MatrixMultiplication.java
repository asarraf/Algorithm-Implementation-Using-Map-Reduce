/**
 * DEVELOPER: ANKIT SARRAF
 * ABOUT: This project finds the Matrix Multiplication of 3 X 3 Integer Matrices
 */

package matrixmultiplication;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MatrixMultiplication {
	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "JobName");

		try {
			// Delete the output folder if it already exists
			deleteFolder(conf, Constants.outputFilePath);
			deleteFolder(conf, Constants.finalResultPath);
			
			// Method to call the Map, Combine and Reduce Task
			myMCRTask(job);
			
			new MatrixCProcessing().getMatrixC(
					Constants.outputFilePath + "/part-r-00000", Constants.finalResultPath);
			
			System.out.println("OPERATION SUCCESSFUL");
			System.out.println("FINAL OUTPUT WRITTEN TO FILE: " + Constants.finalResultPath);
		} catch(Exception e) {
			System.out.println("Cannot Continue ! Some Issue");
			System.out.println("Reason => " + e.getMessage());
		}
	}

	private static void myMCRTask(Job job)
			throws IllegalArgumentException,
			IOException,
			ClassNotFoundException,
			InterruptedException {
		job.setJarByClass(matrixmultiplication.MatrixMultiplication.class);

		// Define the MCR Classes
		job.setMapperClass(MatrixMultiplicationMapper.class);
		job.setCombinerClass(MatrixMultiplicationCombiner.class);
		job.setReducerClass(MatrixMultiplicationReducer.class);

		// Mapper Output Key Value Types
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		// Combiner Output Key Value Types
		// Check this later

		// Reducer Output Key Value Types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.setInputPaths(job, new Path(Constants.inputFilePath));
		FileOutputFormat.setOutputPath(job, new Path(Constants.outputFilePath));

		if (!job.waitForCompletion(true))
			return;
		
		// Once the Reduce Task has been done, Process the Reducer Output file
		// TODO: Create new File for Final Output Matrix
	}

	private static void deleteFolder(Configuration conf, String folderPath) throws IOException {
		// Delete the Folder
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(folderPath);
		if(fs.exists(path)) {
			fs.delete(path,true);
		}
	}
}

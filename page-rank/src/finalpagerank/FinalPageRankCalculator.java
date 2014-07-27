package finalpagerank;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pagerank.PageRank;

public class FinalPageRankCalculator {
	private static final transient Logger LOG = LoggerFactory.getLogger(PageRank.class);

	void usage () {
		System.out.println("Usage : HadoopDFSFileReadWrite <inputfile> <output file>");
		System.exit(1);
	}

	void printErrorMessage(String str) {
		LOG.info(str);
		return;
	}

	public void getFinalPageRank(String inputFileName, String outputFileName) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);

		// Hadoop DFS deals with Path
		Path inFile = new Path(inputFileName);
		Path outFile = new Path(outputFileName);

		// Check if input/output are valid
		if (!fs.exists(inFile)) {
			printErrorMessage("Input file not found");
			throw new IOException();
		} if (!fs.isFile(inFile)) {
			printErrorMessage("Input should be a file");
			throw new IOException();
		} if (fs.exists(outFile)) {
			printErrorMessage("Final Page Rank Input File Exists. Deleting it");
			fs.delete(outFile, true);
		}

		BufferedReader in = new BufferedReader(new InputStreamReader(fs.open(inFile)));
		FSDataOutputStream out = fs.create(outFile);
		
		double [][] tempAdjacency = null;
		String [] stringAdjacency = null;
		int counter = 0;

		try{
			String line;
			while((line = in.readLine()) != null) {
				// Line has the pre-processed output
				line = (line.split("\\s+"))[1];

				if(tempAdjacency == null || stringAdjacency == null) {
					tempAdjacency = new double[line.split(":").length][line.split(":").length];
					stringAdjacency = new String[line.split(":").length];
				}

				stringAdjacency[counter] = line;
				counter++;
			}

			LOG.info("COUNTER = " + counter);

			// This takes O(n^2) : Need a way to reduce the time
			for(int i = 0 ; i < counter ; i++) {
				String [] stringTempAdjacency = stringAdjacency[i].split(":");
				for(int j = 0 ; j < stringTempAdjacency.length ; j++) {
					tempAdjacency[i][j] = Double.parseDouble(stringTempAdjacency[j]);
				}				
			}
			
			double [] sum = new double[counter];
			
			for(int i = 0 ; i < counter ; i++) {
				for(int j = 0 ; j < counter ; j++) {
					sum[i] += tempAdjacency[j][i];
				}
			}
			
			// Now add to this to the final Page Rank File
			for(int i = 0 ; i < counter ; i++) {
				out.writeBytes(i + "\t" + sum[i] + "\n");
			}
		} catch(Exception e) {
			LOG.info("Un-understandable Exception : " + e.getMessage());
		} finally {
			in.close();
			out.close();
		}
	}
}
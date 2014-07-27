package filepreprocess;
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

public class HadoopDFSFileReadWrite {
	private static final transient Logger LOG = LoggerFactory.getLogger(PageRank.class);

	void usage () {
		System.out.println("Usage : HadoopDFSFileReadWrite <inputfile> <output file>");
		System.exit(1);
	}

	void printErrorMessage(String str) {
		LOG.info(str);
		return;
	}

	public void preprocess(String inputFileName, String outputFileName) throws IOException {
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
			printErrorMessage("Page Rank Input File Exists. Deleting it");
			fs.delete(outFile, true);
		}

		BufferedReader in = new BufferedReader(new InputStreamReader(fs.open(inFile)));

		FSDataOutputStream out = fs.create(outFile);
		LOG.info("Opening the output file successful");

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

				int onesInThisRow = 0;
				stringAdjacency[i] = new String("");

				for(int j = 0 ; j < stringTempAdjacency.length ; j++) {
					//LOG.info("[" + i + ", " + j + "] : " + stringTempAdjacency[j]);

					if(stringTempAdjacency[j].equals("1")) {
						onesInThisRow++;
					}

					tempAdjacency[i][j] = Double.parseDouble(stringTempAdjacency[j]);
					tempAdjacency[i][j] = tempAdjacency[i][j] / counter;					
				}

				for(int j = 0 ; j < stringTempAdjacency.length ; j++) {
					tempAdjacency[i][j] = tempAdjacency[i][j] / (double) onesInThisRow;
					stringAdjacency[i] = new String(
							stringAdjacency[i].concat(tempAdjacency[i][j] + ":"));
				}
			}

			for(int i = 0 ; i < counter ; i++) {
				out.writeBytes((i+1) + "\t" + stringAdjacency[i]);
				if(i != (counter - 1)) {
					out.writeBytes("\n");
				}
			}
		} catch(Exception e) {
			LOG.info("Un-understandable Exception : " + e.getMessage());
		} finally {
			in.close();
			out.close();
		}
	}
}
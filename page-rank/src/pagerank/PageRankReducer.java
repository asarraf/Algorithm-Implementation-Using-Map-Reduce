package pagerank;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pagerank.PageRank.MyCounter;

public class PageRankReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
	private static final transient Logger LOG = LoggerFactory.getLogger(PageRank.class);

	public void reduce(IntWritable keyNode, Iterable<Text> nodeDistances, Context context)
			throws IOException, InterruptedException {

		LOG.info("Start Reducer Activity");

		// Find the min distance per reducer
		double sumOfInLinks = 0.0;

		// The String storing the Adjacency List
		String [] adjList = new String[PageRank.TOTALNODES];

		for (Text nodeDistance : nodeDistances) {
			if(nodeDistance.toString().contains(":")) {
				// It is the Adjacency List (The complex Data Structure)
				// In the form "OutLinkRank1:OutLinkRank2:...:OutLinkRankN:"
				adjList = nodeDistance.toString().split(":");
			} else {
				sumOfInLinks += Double.parseDouble(nodeDistance.toString());
			}
		}

		// Count number of the Nodes for which there is a outLink from this node
		int count = 0;
		for(String outLinks : adjList) {
			if(!outLinks.equals("0.0")) {
				count++; 
			}
		}

		// Replace all the non-zero Out Links with the Page Ranks
		String [] newAdjList = new String[PageRank.TOTALNODES];

		for(int i = 0 ; i < adjList.length ; i++) {
			if(adjList[i].equals("0.0")) {
				newAdjList[i] = "0.0";
			} else {
				double newRank = sumOfInLinks / count;
				newAdjList[i] = new String(newRank + "");
			}
		}

		String finalAdjList = new String("");
		for(String aList : newAdjList) {
			finalAdjList = new String(finalAdjList.concat(aList.concat(":")));
		}

		for(int i = 0 ; i < adjList.length ; i++) {
			if((Double.parseDouble(adjList[i]) - Double.parseDouble(newAdjList[i])) > 0.0001) {
				// If the change in Rank is high (Greater than 0.0001)
				context.getCounter(MyCounter.COUNTER).increment(1);
				break;
			}
		}

		LOG.info("REDUCER EMITS : <" + keyNode.get() + " , " + finalAdjList + ">");

		context.write(keyNode, new Text(finalAdjList));
	}
}

package pagerank;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PageRankMapper extends
Mapper<Object, Text, IntWritable, Text> {

	private static final transient Logger LOG = LoggerFactory.getLogger(PageRank.class);

	public void map(Object ikey, Text adjList, Context context)
			throws IOException, InterruptedException {

		LOG.info("Start Mapper Activity");

		// Split the strings along Space
		String [] inputAdj = adjList.toString().split("\\s+");

		IntWritable keyNode = new IntWritable(Integer.parseInt(inputAdj[0]));

		// Display the Key Value Pair Emitted
		LOG.info("<" + keyNode.get() + " , " + inputAdj[1] + ">");

		// Emit the Key Value Pair
		context.write(keyNode, new Text(inputAdj[1]));

		// Split the second part of the Adjacency List along ":"
		String [] inLinks = inputAdj[1].split(":");

		for(int i = 0 ; i < inLinks.length ; i++) {
			// Display the Key Value pair Emitted
			LOG.info("<" + (i + 1) + " , " + inLinks[i] + ">");

			// Emit Key Value Pairs
			context.write(new IntWritable(i + 1), new Text(inLinks[i]));
		}
	}
}
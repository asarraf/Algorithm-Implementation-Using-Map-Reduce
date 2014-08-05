package invertedindex;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class InvertedIndexMapper extends Mapper<Object, Text, Text, Text> {

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		// To know which document I am currently reading
		String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();

		// Get each key word in the Document that is being read
		String [] indexKeys = value.toString().split("\\s+");

		for(String indexKey : indexKeys) {
			// Emit the <keyWords, Document Name> as <Key, Value> pair
			context.write(new Text(indexKey.toLowerCase()), new Text(fileName));
		}
	}
}
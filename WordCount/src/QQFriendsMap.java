import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class QQFriendsMap extends Mapper<LongWritable, Text, Text, Text>{
	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		String line=value.toString();
		String[] ss=line.split("\t");
		/**
		 * map:key 主 value 从，key 从 value 主
		 */
		context.write(new Text(ss[0]), new Text(ss[1]));
		context.write(new Text(ss[1]), new Text(ss[0]));
		
	}
}

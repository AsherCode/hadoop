import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class QQFriendsReduce extends Reducer<Text, Text, Text, Text>{
	@Override
	protected void reduce(Text key, Iterable<Text> i,Context context) throws IOException,
			InterruptedException {
		Set<String> set=new HashSet<String>();
		for(Text t:i){
			set.add(t.toString());
		}
		if(set.size()>1){
			for(Iterator j=set.iterator();j.hasNext();){
				String name=(String) j.next();
				for(Iterator k=set.iterator();k.hasNext();){
					String other=(String) k.next();
					if(!name.equals(other)){
						context.write(new Text(name), new Text(other));
					}
				}
			}
		}
	}
}

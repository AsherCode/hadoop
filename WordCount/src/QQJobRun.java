import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class QQJobRun {
	public static void main(String[] args) {
		Configuration conf=new Configuration();
		conf.set("fs.default.name", "hdfs://node1:9000");
		conf.set("mapred.job.tracker", "node1:9001");
		conf.set("mapred.jar", "C:\\QQFriends.jar");//将该项目打包到C:,若远程调试将jar及源代码打成jar包放到node1的~下，cd /home/hadoop-1.2/bin并./hadoop jar ~/MapReduce.jar com.wangjian.mr.JobRun查看生成的part-r-00000分词结果
		try{
			Job job=new Job(conf);
			job.setJarByClass(QQJobRun.class);
			job.setMapperClass(QQFriendsMap.class);
			job.setReducerClass(QQFriendsReduce.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			//job.setNumReduceTasks(1);设置reduce任务的个数
			FileInputFormat.addInputPath(job, new Path("/usr/input/QQ/"));//mapreduce输入数据所在目录或文件
			FileOutputFormat.setOutputPath(job, new Path("/usr/output/QQ"));//mapreduce执行之后的数据数据的目录
			System.exit(job.waitForCompletion(true)?0:1);
		}catch(Exception e){
			e.printStackTrace();
		}
		
	}
}

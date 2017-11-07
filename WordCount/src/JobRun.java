

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
 

public class JobRun {
	public static void main(String[] args) {
		Configuration conf=new Configuration();
		conf.set("mapred.job.tracker", "node1:9001");
		try{
			Job job=new Job(conf);
			job.setJarByClass(JobRun.class);
			job.setMapperClass(MapTest.class);
			job.setReducerClass(ReduceTest.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
			//job.setNumReduceTasks(1);设置reduce任务的个数
			FileInputFormat.addInputPath(job, new Path("/usr/input/MapReduce/"));//mapreduce输入数据所在目录或文件
			FileOutputFormat.setOutputPath(job, new Path("/usr/output/MapReduce"));//mapreduce执行之后的数据数据的目录
			System.exit(job.waitForCompletion(true)?0:1);
		}catch(Exception e){
			e.printStackTrace();
		}
		
	}
}

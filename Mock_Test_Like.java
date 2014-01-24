	


import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import java.io.IOException;
import java.util.Iterator;

	
public class Mock_Test_Like extends Configured implements Tool
 {

	public int run(String[] args) throws IOException {

	
		String uri="hdfs://localhost:54310/user/hadoop/input/test_data.tsv";
		String uri1="hdfs://localhost:54310/user/hadoop/output/test_result.tsv";
		JobConf conf = new JobConf(Mock_Test_Like.class);
		 	     conf.setJobName("MR");
		 	
		 	     conf.setOutputKeyClass(Text.class);
		 	     conf.setOutputValueClass(IntWritable.class);
		 	
		 	     conf.setMapperClass(Map.class);
		 	     conf.setCombinerClass(Reduce.class);
		 	     conf.setReducerClass(Reduce.class);
		 	
		 	     conf.setInputFormat(TextInputFormat.class);
		 	     conf.setOutputFormat(TextOutputFormat.class);
		 	
		 	     FileInputFormat.setInputPaths(conf, new Path(uri));
		 	    FileOutputFormat.setOutputPath(conf, new Path(uri1));
		 	    JobClient.runJob(conf);
		return 0;
        }
	public static class Map extends MapReduceBase implements Mapper <LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1); 

		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException
		{		
			String Record= new String(value.toString());
		
	        System.out.println(Record);
	        Parser p= new Parser(Record);
			     
	            output.collect ( new Text(p.type), one);
		 	
	}
	}
	public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
    public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			int count = 0;
			while (values.hasNext()) {
			values.next();
			count++;
			}
			System.out.println(key+"/t"+count);
			output.collect(key, new IntWritable(count));

		}
	}


		
	
}

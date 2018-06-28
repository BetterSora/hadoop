package mr.orderTopn.GroupingComparator;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class OrderTopn {

	public static class OrderTopnMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable> {
		OrderBean orderBean = new OrderBean();
		NullWritable v = NullWritable.get();

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			String[] fields = value.toString().split(",");
			
			orderBean.set(fields[0], fields[1], fields[2], Float.parseFloat(fields[3]), Integer.parseInt(fields[4]));
			
			context.write(orderBean,v);
		}
	}

	public static class OrderTopnReducer extends Reducer< OrderBean, NullWritable,  OrderBean, NullWritable>{
		
		/**
		 * 虽然reduce方法中的参数key只有一个，但是只要迭代器迭代一次，key中的值就会变
		 */
		@Override
		protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
			int i = 0;
			for (NullWritable v : values) {
				context.write(key, v);
				if(++i == 2) return;
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);

		job.setJarByClass(OrderTopn.class);

		job.setMapperClass(OrderTopnMapper.class);
		job.setReducerClass(OrderTopnReducer.class);
		
		job.setPartitionerClass(OrderIdPartitioner.class);
		job.setGroupingComparatorClass(OrderIdGroupingComparator.class);
		
		job.setNumReduceTasks(2);

		job.setMapOutputKeyClass(OrderBean.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		job.setOutputKeyClass(OrderBean.class);
		job.setOutputValueClass(NullWritable.class);

		FileInputFormat.setInputPaths(job, new Path("F:\\java-proj\\ideaProj\\hadoop\\data\\order\\input\\order.txt"));
		FileOutputFormat.setOutputPath(job, new Path("F:\\java-proj\\ideaProj\\hadoop\\data\\order\\output2"));

		job.waitForCompletion(true);
	}
	
}

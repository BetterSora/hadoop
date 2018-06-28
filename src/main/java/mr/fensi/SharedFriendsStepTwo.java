package mr.fensi;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SharedFriendsStepTwo {

	static class SharedFriendsStepTwoMapper extends Mapper<LongWritable, Text, Text, Text> {
		Text k = new Text();
		Text v = new Text();

		// A-B C
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] fields = line.split("\t");
			k.set(fields[0]);
			v.set(fields[1]);

			context.write(k, v);
		}
	}

	static class SharedFriendsStepTwoReducer extends Reducer<Text, Text, Text, Text> {
		Text v = new Text();

		@Override
		protected void reduce(Text person_person, Iterable<Text> friends, Context context) throws IOException, InterruptedException {

			StringBuffer sb = new StringBuffer();

			for (Text friend : friends) {
				sb.append(friend).append(" ");

			}

			v.set(sb.toString());
			context.write(person_person, v);
		}

	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);
		job.setJarByClass(SharedFriendsStepTwo.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(SharedFriendsStepTwoMapper.class);
		job.setReducerClass(SharedFriendsStepTwoReducer.class);

		FileInputFormat.setInputPaths(job, new Path("data/friends/output/part-r-00000"));
		FileOutputFormat.setOutputPath(job, new Path("data/friends/output2"));

		job.waitForCompletion(true);

	}

}

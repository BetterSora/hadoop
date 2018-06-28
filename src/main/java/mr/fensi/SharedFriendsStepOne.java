package mr.fensi;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SharedFriendsStepOne {

	static class SharedFriendsStepOneMapper extends Mapper<LongWritable, Text, Text, Text> {
		Text k = new Text();
		Text v = new Text();

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// A:B,C,D,F,E,O
			String line = value.toString();
			String[] person_friends = line.split(":");
			String person = person_friends[0];
			String friends = person_friends[1];
			v.set(person);

			for (String friend : friends.split(",")) {
				k.set(friend);
				// 输出<好友，人>
				context.write(k, v);
			}
		}
	}

	static class SharedFriendsStepOneReducer extends Reducer<Text, Text, Text, Text> {
		Text k = new Text();
		Text v = new Text();

		@Override
		protected void reduce(Text friend, Iterable<Text> persons, Context context) throws IOException, InterruptedException {
			List<String> list = new ArrayList<>();

			for (Text person : persons) {
				list.add(person.toString());

			}

			Collections.sort(list);
			v.set(friend);
			for (int i = 0; i < list.size() - 1; i++) {
				for (int j = i + 1; j < list.size(); j++) {
					k.set(list.get(i) + "-" + list.get(j));
					// A-B C
					context.write(k, v);
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);
		job.setJarByClass(SharedFriendsStepOne.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapperClass(SharedFriendsStepOneMapper.class);
		job.setReducerClass(SharedFriendsStepOneReducer.class);

		FileInputFormat.setInputPaths(job, new Path("data/friends/input"));
		FileOutputFormat.setOutputPath(job, new Path("data/friends/output"));

		job.waitForCompletion(true);
	}
}

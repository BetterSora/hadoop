package mr.flow;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 对日志数据中的上下行流量信息汇总，并输出按照总流量倒序排序的结果
 * @author Qin
 */
public class FlowCount {
    static class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split("\t");
            String phoneNumber = fields[1];
            long upFlow = Long.parseLong(fields[fields.length-3]);
            long dFlow = Long.parseLong(fields[fields.length-2]);

            context.write(new Text(phoneNumber), new FlowBean(upFlow, dFlow));
        }
    }

    static class FlowCountReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
        private FlowBean bean = new FlowBean();

        @Override
        protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
            long sum_upFlow = 0;
            long sum_dFlow = 0;

            for (FlowBean value : values) {
                sum_upFlow += value.getUpFlow();
                sum_dFlow += value.getdFlow();
            }
            bean.set(sum_upFlow, sum_dFlow);
            context.write(key, bean);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapreduce.framework.name", "local");
        conf.set("fs.defaultFS", "file:///");

        Job job = Job.getInstance(conf);

        job.setJarByClass(FlowCount.class);

        job.setMapperClass(FlowCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        job.setReducerClass(FlowCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        job.setPartitionerClass(ProvincePartitioner.class);
        job.setNumReduceTasks(5);

        FileInputFormat.setInputPaths(job, new Path("./data/flowcount/input/flowcount.txt"));
        FileOutputFormat.setOutputPath(job, new Path("./data/flowcount/output"));

        boolean res = job.waitForCompletion(true);
        System.exit(res ? 0 : 1);
    }
}

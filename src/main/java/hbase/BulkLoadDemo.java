package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 使用BulkLoad向HBase中批量导入数据
 */
public class BulkLoadDemo {
    static class BulkLoadMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // 1,zhangsan,20
            String line = value.toString();
            String[] fields = line.split(",");
            String rowKey = fields[0];
            String name = fields[1];
            String age = fields[2];

            ImmutableBytesWritable ibw = new ImmutableBytesWritable(rowKey.getBytes());

            Put put = new Put(rowKey.getBytes());
            put.addColumn("info".getBytes(), "name".getBytes(), name.getBytes());
            put.addColumn("info".getBytes(), "age".getBytes(), age.getBytes());

            context.write(ibw, put);
        }
    }

    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "qinzhen");

        final String SRC_PATH = "hdfs://hadoop:8020/test.txt";
        final String DESC_PATH = "hdfs://hadoop:8020/out";

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(BulkLoadDemo.class);
        job.setMapperClass(BulkLoadMapper.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);

        job.setOutputFormatClass(HFileOutputFormat2.class);
        FileInputFormat.addInputPath(job, new Path(SRC_PATH));
        FileOutputFormat.setOutputPath(job, new Path(DESC_PATH));

        // HBase配置文件
        Configuration hConf = HBaseConfiguration.create();
        hConf.set("hbase.zookeeper.quorum", "hadoop:2181");
        Connection conn = ConnectionFactory.createConnection(hConf);
        Table table = conn.getTable(TableName.valueOf("user_info"));
        HFileOutputFormat2.configureIncrementalLoad(job, (HTable) table);

        job.waitForCompletion(true);
        LoadIncrementalHFiles loader = new LoadIncrementalHFiles(hConf);
        loader.doBulkLoad(new Path(DESC_PATH), (HTable) table);
    }
}

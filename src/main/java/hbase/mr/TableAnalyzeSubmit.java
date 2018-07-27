package hbase.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;


/**
 * 利用MapReduce对HBase数据进行统计分析，并将结果写入HBase表中
 *
 * 参数
 * user_info info name user_count
 *
 */
public class TableAnalyzeSubmit {
    public static void main(String[] args) throws Exception {
        String tableName = args[0];
        String family = args[1];
        String column = args[2];
        String targetTable = args[3];
        System.out.println("tableName=" + tableName + ", family=" + family + ", column=" + column + ", targetTable="
                + targetTable);

        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "hadoop:2181");
        Scan scan = new Scan();
        // 设置每次读取的行数
        scan.setCaching(500);
        // 告诉HBase本次扫描的数据不要放入缓存中
        scan.setCacheBlocks(false);
        // 本次扫描的列族和列
        scan.addColumn(Bytes.toBytes(family), Bytes.toBytes(column));

        Job job = Job.getInstance(conf, "analyze table data for " + tableName);
        job.setJarByClass(TableAnalyzeSubmit.class);
        // 设置source表名
        TableMapReduceUtil.initTableMapperJob(Bytes.toBytes(tableName), scan, TableAnalyzeMap.class, Text.class, IntWritable.class, job);
        // 设置目标表名
        TableMapReduceUtil.initTableReducerJob(targetTable, TableAnalyzeReduce.class, job);

        job.setMapperClass(TableAnalyzeMap.class);
        job.setReducerClass(TableAnalyzeReduce.class);
        job.setNumReduceTasks(1);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

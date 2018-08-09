package hbase.mr;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * Reduce业务逻辑
 * KeyIn ValueIn KeyOut
 */
public class TableAnalyzeReduce extends TableReducer<Text, IntWritable, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int i = 0;
        for (IntWritable value : values) {
            i += value.get();
        }

        // 以名字作为RowKey
        Put put = new Put(key.toString().getBytes());
        put.addColumn("info".getBytes(), "count".getBytes(), Bytes.toBytes(i));

        context.write(NullWritable.get(), put);
    }
}

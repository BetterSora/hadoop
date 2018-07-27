package hbase.mr;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * 表结构
 * 1 info:name zhangsan info:age 24
 * 2 info:name lisi info:age 26
 * 3 info:name wangwu info:age 22
 *
 * Map业务逻辑: 统计每个名字出现的次数
 * 输出key: 名字
 * 输出value: 次数1
 */
public class TableAnalyzeMap extends TableMapper<Text, IntWritable> {
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        CellScanner scanner = value.cellScanner();
        while (scanner.advance()) {
            Cell cell = scanner.current();
            String qualifier = new String(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
            String colValue = new String(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());

            System.out.println(qualifier + "-" + colValue);
            context.write(new Text(colValue), new IntWritable(1));
        }
        /*// 获取name的value
        byte[] name = value.getValue("info".getBytes(), "name".getBytes());

        context.write(new Text(new String(name)), new IntWritable(1));*/
    }
}

package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class HBaseClientDML {
    Connection conn = null;

    /**
     * 构建连接对象
     */
    @Before
    public void getConn() throws Exception {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "hadoop:2181");
        conn = ConnectionFactory.createConnection(conf);
    }

    /**
     * 添加数据
     * 修改数据:put来覆盖
     */
    @Test
    public void testPut() throws Exception {
        // 获取一个操作指定表的table对象,进行DML操作
        Table table = conn.getTable(TableName.valueOf("user_info"));

        // 构造要插入的数据为一个Put类型(一个put对象只能对应一个rowkey)的对象
        Put put1 = new Put(Bytes.toBytes("001"));
        put1.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("username"), Bytes.toBytes("张三"));
        put1.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("age"), Bytes.toBytes("28"));
        put1.addColumn(Bytes.toBytes("extra_info"), Bytes.toBytes("address"), Bytes.toBytes("北京"));

        Put put2 = new Put(Bytes.toBytes("002"));
        put2.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("username"), Bytes.toBytes("李四"));
        put2.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("age"), Bytes.toBytes("38"));
        put2.addColumn(Bytes.toBytes("extra_info"), Bytes.toBytes("address"), Bytes.toBytes("上海"));

        List<Put> puts = new ArrayList<>();
        puts.add(put1);
        puts.add(put2);

        // 插入数据
        table.put(puts);

        table.close();
    }

    /**
     * 删除数据
     */
    @Test
    public void testDelete() throws Exception {
        Table table = conn.getTable(TableName.valueOf("user_info"));

        // 构造一个对象封装要删除的数据信息
        Delete delete1 = new Delete(Bytes.toBytes("001"));
        delete1.addColumn(Bytes.toBytes("extra_info"), Bytes.toBytes("address"));

        Delete delete2 = new Delete(Bytes.toBytes("002"));

        List<Delete> dels = new ArrayList<>();
        dels.add(delete1);
        dels.add(delete2);

        table.delete(dels);

        table.close();
    }

    /**
     * 查询数据
     */
    @Test
    public void testGet() throws Exception {
        Table table = conn.getTable(TableName.valueOf("user_info"));

        Get get = new Get(Bytes.toBytes("001"));

        Result result = table.get(get);

        // 从结果中取用户指定的某个key的value
        byte[] value = result.getValue(Bytes.toBytes("base_info"), Bytes.toBytes("age"));
        System.out.println(new String(value));

        System.out.println("-------------------------");

        // 遍历整行结果中的所有kv单元格
        CellScanner scanner = result.cellScanner();
        while (scanner.advance()) {
            Cell cell = scanner.current();

            byte[] rowArray = cell.getRowArray();
            byte[] familyArray = cell.getFamilyArray();
            byte[] qualifierArray = cell.getQualifierArray();
            byte[] valueArray = cell.getValueArray();

            System.out.println("行键: " + new String(rowArray, cell.getRowOffset(), cell.getRowLength()));
            System.out.println("列族名: " + new String(familyArray, cell.getFamilyOffset(), cell.getFamilyLength()));
            System.out.println("列名: " + new String(qualifierArray, cell.getQualifierOffset(), cell.getQualifierLength()));
            System.out.println("value: " + new String(valueArray, cell.getValueOffset(), cell.getValueLength()));
        }

        table.close();
    }

    /**
     * 循环插入大量数据
     */
    @Test
    public void testManyPuts() throws Exception {
        Table table = conn.getTable(TableName.valueOf("user_info"));

        ArrayList<Put> puts = new ArrayList<>();

        for (int i = 0; i < 100000; i++) {
            Put put = new Put(Bytes.toBytes("" + i));
            put.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("username"), Bytes.toBytes("张三" + i));
            put.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("age"), Bytes.toBytes((18 + i) + ""));
            put.addColumn(Bytes.toBytes("extra_info"), Bytes.toBytes("address"), Bytes.toBytes("北京"));

            puts.add(put);
        }

        table.put(puts);
    }

    /**
     * 按行键范围查询数据
     */
    @Test
    public void testScan() throws Exception {
        Table table = conn.getTable(TableName.valueOf("user_info"));

        Scan scan = new Scan(Bytes.toBytes("10"), Bytes.toBytes("10000\000"));

        ResultScanner scanner = table.getScanner(scan);
        Iterator<Result> iterator = scanner.iterator();

        // 遍历行键
        while (iterator.hasNext()) {
            Result result = iterator.next();

            // 遍历整行结果中的所有kv单元格
            CellScanner cellScanner = result.cellScanner();
            while (cellScanner.advance()) {
                Cell cell = cellScanner.current();

                byte[] rowArray = cell.getRowArray();  //本kv所属的行键的字节数组
                byte[] familyArray = cell.getFamilyArray();  //列族名的字节数组
                byte[] qualifierArray = cell.getQualifierArray();  //列名的字节数据
                byte[] valueArray = cell.getValueArray(); // value的字节数组

                System.out.println("行键: " + new String(rowArray, cell.getRowOffset(), cell.getRowLength()));
                System.out.println("列族名: " + new String(familyArray, cell.getFamilyOffset(), cell.getFamilyLength()));
                System.out.println("列名: " + new String(qualifierArray, cell.getQualifierOffset(), cell.getQualifierLength()));
                System.out.println("value: " + new String(valueArray, cell.getValueOffset(), cell.getValueLength()));
            }

            System.out.println("----------------------");
        }
    }

    /**
     * 断开连接
     */
    @After
    public void closeConn() throws Exception {
        conn.close();
    }
}

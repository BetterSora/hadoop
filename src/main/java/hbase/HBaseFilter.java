package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Iterator;

/**
 * HBase过滤器查询测试
 */
public class HBaseFilter {
    private Configuration conf = null;
    private Connection conn = null;

    @Before
    public void getConnection() throws IOException {
        conf = HBaseConfiguration.create();
        // 设置ZK地址
        conf.set("hbase.zookeeper.quorum", "hadoop:2181");

        conn = ConnectionFactory.createConnection(conf);
    }

    /**
     * 行键过滤器 RowFilter
     */
    @Test
    public void testRowFilter() throws Exception {
        // 获取HBase中表
        Table table = conn.getTable(TableName.valueOf("user_info"));

        Scan scan = new Scan();
        // 创建过滤器对象
        //RowFilter filter = new RowFilter(CompareOp.EQUAL, new SubstringComparator("01")); // 行键中包含01字符的
        RowFilter filter = new RowFilter(CompareOp.EQUAL, new BinaryComparator("01".getBytes())); // 行键等于01的
        // 设置过滤器
        scan.setFilter(filter);

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
     * 列族过滤器 FamilyFilter
     */
    @Test
    public void testFamilyFilter() throws Exception {
        Table table = conn.getTable(TableName.valueOf("user_info"));

        Scan scan = new Scan();
        // 创建过滤器对象
        FamilyFilter filter = new FamilyFilter(CompareOp.EQUAL, new BinaryComparator("base_info".getBytes())); // 列族等于base_info的
        // 设置过滤器
        scan.setFilter(filter);

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
     * 列过滤器 QualifierFilter
     */
    @Test
    public void testQualifierFilter() throws Exception {
        Table table = conn.getTable(TableName.valueOf("user_info"));

        Scan scan = new Scan();
        // 创建过滤器对象
        QualifierFilter filter = new QualifierFilter(CompareOp.EQUAL, new BinaryComparator("age".getBytes())); // 列等于age的
        // 设置过滤器
        scan.setFilter(filter);

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
     * 值过滤器 ValueFilter
     */
    @Test
    public void testValueFilter() throws Exception {
        Table table = conn.getTable(TableName.valueOf("user_info"));

        Scan scan = new Scan();
        // 创建过滤器对象
        ValueFilter filter = new ValueFilter(CompareOp.EQUAL, new BinaryComparator("28".getBytes())); // 值等于28的
        // 设置过滤器
        scan.setFilter(filter);

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
     * 单列值过滤器 SingleColumnValueFilter
     * 单列值排除器 SingleColumnValueExcludeFilter -----返回排除了该列的结果 与上面的结果相反
     */
    @Test
    public void testSingleColumnValueFilter() throws Exception {
        Table table = conn.getTable(TableName.valueOf("user_info"));

        Scan scan = new Scan();
        SingleColumnValueFilter filter = new SingleColumnValueFilter("base_info".getBytes(), "age".getBytes(), CompareOp.GREATER_OR_EQUAL, Bytes.toBytes(20));
        // 如果不设置为 true，则那些不包含指定 column 的行也会返回，因为有的行没有这个age列
        filter.setFilterIfMissing(true);
        scan.setFilter(filter);

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
     * 前缀过滤器 PrefixFilter----针对行键
     */
    @Test
    public void testPrefixFilter() throws Exception {
        Table table = conn.getTable(TableName.valueOf("user_info"));

        Scan scan = new Scan();
        PrefixFilter filter = new PrefixFilter("0".getBytes());
        scan.setFilter(filter);

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
     * 列前缀过滤器 ColumnPrefixFilter
     */
    @Test
    public void testColumnPrefixFilter() throws Exception {
        Table table = conn.getTable(TableName.valueOf("user_info"));

        Scan scan = new Scan();
        ColumnPrefixFilter filter = new ColumnPrefixFilter("ge".getBytes());
        scan.setFilter(filter);

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
     * 分页过滤器 PageFilter
     * 分页过滤器：对HBase中的数据，按照所设置的一页的行数，进行分页。
     */
    @Test
    public void testPageFilter() throws Exception {
        Table table = conn.getTable(TableName.valueOf("user_info"));
        byte[] POSTFIX = new byte[] { 0x00 }; // 长度为零的字节数组
        PageFilter filter = new PageFilter(1);

        // 我们要了解分页过滤的机制
        // 通过分页过滤，它会保证每页会存在所设置的行数（我们假设为3），也就是说如果把首行设置为一个不存在的行键也没关系
        // 这样就会将该行键后的3行放到页中，而如果首行存在，这时就会将首行和该行后面的两行放到一页中
        int totalRows = 0; // 总行数
        byte[] lastRow = null; // 该页的最后一行
        while (true) {
            Scan scan = new Scan();
            scan.setFilter(filter);
            //如果不是第一页
            if (lastRow != null) {
                // 因为不是第一页，所以我们需要设置其实位置，我们在上一页最后一个行键后面加了一个零，来充当上一页最后一个行键的下一个
                byte[] startRow = Bytes.add(lastRow, POSTFIX);
                System.out.println("start row: " + Bytes.toStringBinary(startRow));
                scan.setStartRow(startRow);
            }

            ResultScanner scanner = table.getScanner(scan);
            int localRows = 0;
            Result result;
            while ((result = scanner.next()) != null) {
                System.out.println(localRows++ + ": " + result);
                totalRows++;
                lastRow = result.getRow(); // 获取最后一行的行键
            }

            scanner.close();
            // 最后一页，查询结束
            if (localRows == 0)
                break;
        }

        System.out.println("total rows: " + totalRows);
        table.close();
    }

        @After
        public void close () throws IOException {
            conn.close();
        }
    }

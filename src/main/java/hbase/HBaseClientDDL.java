package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class HBaseClientDDL {
    Connection conn = null;

    /**
     * 获取HBase连接
     */
    @Before
    public void getConn() throws Exception {
        // 会自动加载hbase-site.xml
        Configuration conf = HBaseConfiguration.create();
        // 设置ZooKeeper地址
        conf.set("hbase.zookeeper.quorum", "hadoop:2181");

        conn = ConnectionFactory.createConnection(conf);
    }

    /**
     * 建表
     */
    @Test
    public void testCreateTable() throws Exception {
        // 从连接中拿到DDL管理器
        Admin admin = conn.getAdmin();

        // 创建表描述信息
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf("user_info"));

        // 创建列族描述信息
        HColumnDescriptor family1 = new HColumnDescriptor("base_info");
        family1.setMaxVersions(3); // 设置列族中存储数据的最大版本数，默认是1
        HColumnDescriptor family2 = new HColumnDescriptor("extra_info");

        // 将列族定义信息对象放入表定义对象中
        hTableDescriptor.addFamily(family1);
        hTableDescriptor.addFamily(family2);

        // 用ddl操作器对象：admin 来建表
        admin.createTable(hTableDescriptor);

        admin.close();
    }

    /**
     * 删除表
     */
    @Test
    public void testDropTable() throws Exception {
        Admin admin = conn.getAdmin();

        // 停用表
        admin.disableTable(TableName.valueOf("user_info"));
        // 删除表
        admin.deleteTable(TableName.valueOf("user_info"));

        admin.close();
    }

    /**
     * 修改表定义
     */
    @Test
    public void testAlterTable() throws Exception {
        Admin admin = conn.getAdmin();

        // 取出旧的表定义信息
        HTableDescriptor hTableDescriptor = admin.getTableDescriptor(TableName.valueOf("user_info"));

        // 新构造一个列族定义
        HColumnDescriptor hColumnDescriptor = new HColumnDescriptor("other_info");
        hColumnDescriptor.setBloomFilterType(BloomType.ROWCOL); // 设置该列族的布隆过滤器类型

        // 将列族定义添加到表定义对象中
        hTableDescriptor.addFamily(hColumnDescriptor);

        // 将修改过的表定义交给admin去提交
        admin.modifyTable(TableName.valueOf("user_info"), hTableDescriptor);
    }

    /**
     * 断开连接
     */
    @After
    public void closeConn() throws Exception {
        conn.close();
    }
}

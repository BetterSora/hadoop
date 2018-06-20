package zk;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * ZooKeeper的简单使用
 */
public class ZkDemo {
    public static final String ADDRESS = "hadoop:2181";
    public static final int SESSION_TIMEOUT = 30000;
    public static ZooKeeper zk = null;
    public static CountDownLatch latch = new CountDownLatch(1);

    @Before
    public void before() throws Exception {
        // 构造zk客户端，最后一个参数为收到事件后的回调函数
        zk = new ZooKeeper(ADDRESS, SESSION_TIMEOUT, (WatchedEvent event) -> {System.out.println(event.toString());latch.countDown();});
        // 由于对构造函数的调用是立即返回的，因此在使用新建的ZooKeeper对象之前一定要等待其与ZooKeeper服务之间的连接建立成功
        latch.await();
    }

    @Test
    public void testCreate() throws KeeperException, InterruptedException {
        // 参数1:要创建节点的路径 参数2:节点的数据 参数3:节点的权限  参数4:节点的类型
        String result = zk.create("/zk", "Hello".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        System.out.println(result);
    }

    @Test
    public void testGet() throws KeeperException, InterruptedException {
        byte[] result = zk.getData("/zk", true, null);
        /*Thread.sleep(Integer.MAX_VALUE);*/
        System.out.println(new String(result));
    }

    @Test
    public void testDelete() throws KeeperException, InterruptedException {
        // 参数2:指定要删除的版本，-1表示删除所有版本
        zk.delete("/zk", -1);
    }

    @Test
    public void getChildren() throws KeeperException, InterruptedException {
        List<String> children = zk.getChildren("/hbase", true);
        children.forEach(System.out::println);
    }

    @Test
    public void testExist() throws KeeperException, InterruptedException {
        Stat result = zk.exists("/zk", true);
        System.out.println(result != null ? "true" : "false");
    }

    @Test
    public void testSet() throws KeeperException, InterruptedException {
        zk.setData("/zk", "world".getBytes(), -1);
        testGet();
    }

    @After
    public void after() throws InterruptedException {
        zk.close();
    }
}

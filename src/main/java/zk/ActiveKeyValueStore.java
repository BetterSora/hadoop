package zk;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.data.Stat;

/**
 * 每个znode上存储了一个键／值对，用来完成配置服务
 */
public class ActiveKeyValueStore {
    public static final Charset CHARSET = Charset.forName("UTF-8");
    public static ZooKeeper zk = null;
    public static CountDownLatch latch = new CountDownLatch(1);

    private ActiveKeyValueStore() {}

    public static ActiveKeyValueStore getConnection(String address, int timeout) throws Exception {
        zk = new ZooKeeper(address, timeout, event -> {if (event.getState() == KeeperState.SyncConnected) latch.countDown();});
        latch.await();

        return new ActiveKeyValueStore();
    }

    public void write(String path, String value) throws Exception {
        Stat stat = zk.exists(path, false);
        if (stat == null) {
            zk.create(path, value.getBytes(CHARSET), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        } else {
            zk.setData(path, value.getBytes(CHARSET), -1);
        }
    }

    public String read(String path, Watcher watcher) throws Exception {
        byte[] result = zk.getData(path, watcher, null);

        return new String(result, CHARSET);
    }
}

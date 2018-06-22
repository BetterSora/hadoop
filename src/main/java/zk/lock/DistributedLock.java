package zk.lock;

import org.apache.zookeeper.*;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

/**
 * 利用ZooKeeper实现分布式锁
 */
public class DistributedLock implements Watcher {
    // zk连接
    private ZooKeeper zk = null;
    // 锁的根目录
    private String root = "/locks";
    // 计数器
    private CountDownLatch latch = new CountDownLatch(1);
    // zk地址
    private String address = null;
    // zk连接超时时间
    private int sessionTimeout = 30000;
    // 竞争资源的标识
    private String lockName = null;
    // 等待前一个锁
    private String waitNode = null;
    // 当前锁
    private String myZnode = null;

    public DistributedLock(String address, String lockName) {
        this.lockName = lockName;
        try {
            zk = new ZooKeeper(address, sessionTimeout, this);
            Stat stat = zk.exists(root, false);
            if (stat == null) {
                // 创建根节点
                zk.create(root, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取锁，获取失败就等待
     */
    public void lock() {
        if (tryLock()) {
            System.out.println("Thread " + Thread.currentThread().getId() + " " + myZnode + " get lock true");
        } else {
            waitForLock(waitNode);
        }
    }

    private void waitForLock(String lower) {
        try {
            // 判断比自己小的节点是否存在，并注册监听
            Stat stat = zk.exists(root + "/" + lower, true);

            if (stat != null) {
                System.out.println("Thread " + Thread.currentThread().getId() + " waiting for " + root + "/" + lower);
                latch = new CountDownLatch(1);
                latch.await();
                latch = null;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void unLock() {
        try {
            System.out.println("unlock " + myZnode);
            zk.delete(myZnode, -1);
            myZnode = null;
            zk.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建临时节点并尝试获取锁，获取成功返回true
     */
    private boolean tryLock() {
        //if (lockName.contains(split_str)) throw new Exception("lockName can not contains " + split_str);
        try {
            String split_str = "_lock_";
            // /locks/xxxx_lock_00001
            myZnode = zk.create(root + "/" + lockName + split_str, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            System.out.println(myZnode + " is created!");

            // 取出所有子节点
            List<String> children = zk.getChildren(root, false);
            // 取出所有lockName的锁并按照序号进行排序 xxxx_lock_00001
            List<String> lockObjNodes = children.stream().filter(str -> str.split(split_str)[0].equals(lockName)).sorted().collect(Collectors.toList());

            //System.out.println(myZnode + "==" + lockObjNodes.get(0));
            if (myZnode.equals(root + "/" + lockObjNodes.get(0))) {
                // 如果是最小节点，则表示获取到锁
                return true;
            }

            //如果不是最小的节点，找到比自己小1的节点
            String subMyZnode = myZnode.substring(myZnode.lastIndexOf("/") + 1);
            waitNode = lockObjNodes.get(Collections.binarySearch(lockObjNodes, subMyZnode) - 1);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return false;
    }

    @Override
    public void process(WatchedEvent event) {
        if (latch != null) {
            latch.countDown();
        }
    }

    public static void main(String[] args) {
        List<String> list = new ArrayList<>();
        list.add("a");
        list.add("b");
        list.add("a");
        list.add("c");
        List<String> list2 = list.stream().filter(str -> str.equals("a")).collect(Collectors.toList());
        System.out.println(list2);
    }
}

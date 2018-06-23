package zk;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;

import java.io.IOException;
import java.util.ArrayList;

/**
 * 权限管理
 */
public class ACLTest {
    public static void main(String[] args) throws Exception {
        ArrayList<ACL> acls = new ArrayList<>();
        // 添加第一个id，采用用户名密码形式
        ACL acl1 = new ACL(ZooDefs.Perms.ALL, new Id("digest", DigestAuthenticationProvider.generateDigest("admin:admin")));
        acls.add(acl1);
        //添加第二个id，所有用户可读权限
        ACL acl2 = new ACL(ZooDefs.Perms.READ, new Id("world", "anyone"));
        acls.add(acl2);

        // Zk用admin认证，创建/test ZNode。
        ZooKeeper zk = new ZooKeeper("hadoop:2181", 30000, System.out::println);
        zk.addAuthInfo("digest", "admin:admin".getBytes());
        //zk.create("/test", "data".getBytes(), acls, CreateMode.PERSISTENT);

        zk.setData("/test", "hello".getBytes(), -1);
        byte[] data = zk.getData("/test", false, null);
        System.out.println(new String(data));

        zk.close();
    }
}

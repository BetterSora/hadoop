package drpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.net.InetSocketAddress;

/**
 * RPC客户端
 * @author Qin
 */
public class RPCClient {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();


        long clientVersion = 1L;
        // 在本机拿到了远程的服务
        UserService userService = RPC.getProxy(UserService.class, clientVersion,
                new InetSocketAddress("localhost", 9999), conf);

        userService.addUser("Tom", 20);
        System.out.println("Client Invoked...");
        RPC.stopProxy(userService);
    }
}

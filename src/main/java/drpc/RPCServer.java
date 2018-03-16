package drpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

/**
 * RPC server服务
 * @author Qin
 */
public class RPCServer {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        RPC.Builder builder = new RPC.Builder(conf);

        // Java Builder模式
        RPC.Server server = builder.setProtocol(UserService.class)
                .setInstance(new UserServiceImpl())
                .setBindAddress("localhost")
                .setPort(9999)
                .build();

        server.start();
    }
}

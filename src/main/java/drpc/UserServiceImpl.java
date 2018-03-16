package drpc;

/**
 * 用户的服务接口实现类
 * @author Qin
 */
public class UserServiceImpl implements UserService {

    @Override
    public void addUser(String name, int age) {
        System.out.println("Server Invoked: add user success...., name is :" + name);
    }
}

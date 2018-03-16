package drpc;

/**
 * 用户的服务接口
 * @author Qin
 */
public interface UserService {
    long versionID = 1L;

    /**
     * 添加用户
     * @param name 名字
     * @param age 年龄
     */
    void addUser(String name, int age);
}

package zk.lock;

import zk.lock.ConcurrentTest.ConcurrentTask;

public class ZkTest {
    public static void main(String[] args) {
        Runnable task1 = () -> {
            DistributedLock lock = null;
            try {
                lock = new DistributedLock("hadoop:2181","test1");
                //lock = new DistributedLock("hadoop:2181","test2");
                lock.lock();
                Thread.sleep(3000);
                System.out.println("===Thread " + Thread.currentThread().getId() + " running");
            } catch (Exception e) {
                e.printStackTrace();
            }
            finally {
                if(lock != null)
                    lock.unLock();
            }
        };
        new Thread(task1).start();
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e1) {
            e1.printStackTrace();
        }

        ConcurrentTask[] tasks = new ConcurrentTask[10];
        for(int i = 0; i < tasks.length; i++){
            ConcurrentTask task3 = () -> {
                DistributedLock lock = null;
                try {
                    lock = new DistributedLock("hadoop:2181","test2");
                    lock.lock();
                    System.out.println("Thread " + Thread.currentThread().getId() + " running");
                } catch (Exception e) {
                    e.printStackTrace();
                }
                finally {
                    if(lock != null)
                        lock.unLock();
                }
            };
            tasks[i] = task3;
        }
        new ConcurrentTest(tasks);
    }
}

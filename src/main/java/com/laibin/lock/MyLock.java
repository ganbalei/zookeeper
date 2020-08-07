package com.laibin.lock;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class MyLock {
    private static final String IP = "192.168.2.214:2181";
    private static final String LOCK_ROOT_PATH = "/Locks";
    private static final String LOCK_NODE_NAME = "/Lock";

    private String lockPath;
    private ZooKeeper zooKeeper = null;
    CountDownLatch countDownLatch =new CountDownLatch(1);

    //监视器对象，监视上一个节点是否被删除
    Watcher watcher = new Watcher() {
        public void process(WatchedEvent watchedEvent) {
            if (watchedEvent.getType() == Event.EventType.NodeDeleted){
                synchronized (this){
                    notifyAll();
                }
            }
        }
    };

    //打开zookeeper连接
    public MyLock(){
        try {
            zooKeeper = new ZooKeeper(IP, 5000, new Watcher() {
                public void process(WatchedEvent watchedEvent) {
                    try {
                        if (watchedEvent.getType() == Event.EventType.None) {
                            if (watchedEvent.getState() == Event.KeeperState.SyncConnected) {
                                System.out.println("连接创建成功");
                                countDownLatch.countDown();
                            } else if (watchedEvent.getState() == Event.KeeperState.Disconnected) {
                                System.out.println("断开连接");
                            } else if (watchedEvent.getState() == Event.KeeperState.Expired) {
                                System.out.println("会话超时");
                            } else if (watchedEvent.getState() == Event.KeeperState.AuthFailed) {
                                System.out.println("认证失败");
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //获取锁
    public void acquireLock(){
        try {
            createLock();
            attemptLock();
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    //创建锁节点
    public void createLock() throws KeeperException, InterruptedException {
        //判断LockS是否存在，不存在则创建
        Stat stat = zooKeeper.exists(LOCK_ROOT_PATH, false);
        if (stat == null){
            zooKeeper.create(LOCK_ROOT_PATH, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
        //创建临时有序节点 如/Locks/Lock_000000001
        lockPath = zooKeeper.create(LOCK_ROOT_PATH + "/" + LOCK_NODE_NAME, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println("节点创建成功" + lockPath);
    }

    //尝试获取锁
    public void attemptLock() throws KeeperException, InterruptedException {
        //获取Locks节点下的所有子节点
        List<String> list = zooKeeper.getChildren(LOCK_ROOT_PATH, false);
        //对子节点排序
        Collections.sort(list);
        // 获取创建的临时有序节点
        int index = list.indexOf(lockPath.substring(LOCK_ROOT_PATH.length()+1));
        if (index == 0){
            System.out.println("获取锁成功");
            return;
        } else {
            //上一个节点的路径
            String path = list.get(index - 1);
            Stat stat = zooKeeper.exists(LOCK_ROOT_PATH + "/" + path, watcher);
            if (stat == null){
                attemptLock();
            } else {
                synchronized (watcher){
                    watcher.wait();
                }
                attemptLock();
            }
        }
    }

    //释放锁
    public void releaseLock() throws InterruptedException, KeeperException {
        //删除临时有序节点
        zooKeeper.delete(this.lockPath, -1);
        zooKeeper.close();
        System.out.println("锁已经释放"+ this.lockPath);
    }

}

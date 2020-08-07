package com.laibin.node;

import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

public class ZkUniqueId implements Watcher {

    final String IP = "192.168.2.214:2181";

    CountDownLatch countDownLatch =new CountDownLatch(1);

    private String defaultPath = "uniqueId";

    private ZooKeeper zooKeeper = null;

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

    public ZkUniqueId() throws IOException, InterruptedException {
        zooKeeper = new ZooKeeper(IP, 5000, this);
        countDownLatch.await();
    }

    private String getUniqueId() throws KeeperException, InterruptedException {
        String path = "";
        //uniqueId00000001
        path = zooKeeper.create(defaultPath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        return path.substring(9);
    }

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        ZkUniqueId zkUniqueId = new ZkUniqueId();
        for (int i = 0; i < 10; i++) {
            String uniqueId = zkUniqueId.getUniqueId();
            System.out.println(uniqueId);
        }
    }
}

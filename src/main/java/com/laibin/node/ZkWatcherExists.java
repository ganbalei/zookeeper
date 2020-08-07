package com.laibin.node;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ZkWatcherExists {

    final String IP = "192.168.2.214:2181";
    ZooKeeper zooKeeper = null;

    @Before
    public void before() throws IOException, InterruptedException {
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        //连接客户端
        zooKeeper = new ZooKeeper(IP, 5000, new Watcher() {
            public void process(WatchedEvent watchedEvent) {
                //连接成功
                if (watchedEvent.getState() == Event.KeeperState.SyncConnected) {
                    countDownLatch.countDown();
                }
                System.out.println("path="+ watchedEvent.getPath());
                System.out.println("eventType="+ watchedEvent.getType());
            }
        });
        countDownLatch.await();
    }

    @After
    public void after() throws InterruptedException {
        zooKeeper.close();
    }

    /**
     * 检查节点是否存在
     * 能够捕获的事件类型：
     * NodeCreated 节点创建
     * NodeDeleted 节点删除
     * NodeDataChanged 节点内容发生变化
     * @throws KeeperException
     * @throws InterruptedException
     */
    @Test
    public void watcherExists1() throws KeeperException, InterruptedException {
        //org1：节点的路径
        //org2：使用连接对象的watcher
        zooKeeper.exists("/node1", true);
        TimeUnit.SECONDS.sleep(4);
        System.out.println("结束");
    }

    /**
     * 能够捕获的事件类型:
     * NodeDataChanged 节点内容发生变化
     * NodeDeleted 节点删除
     * @throws KeeperException
     * @throws InterruptedException
     */
    @Test
    public void watcherGetData() throws KeeperException, InterruptedException {
        //开启默认watcher
        zooKeeper.getData("/node1", true, null);
        System.out.println("结束");
        //自定义watcher，是一次性的
        zooKeeper.getData("/node1", new Watcher() {
            public void process(WatchedEvent watchedEvent) {
                System.out.println("自定义watcher");
            }
        }, null);
        //多次
        zooKeeper.getData("/node1", new Watcher() {
            public void process(WatchedEvent watchedEvent) {
                System.out.println("自定义watcher");
                try {
                    if (watchedEvent.getType() == Event.EventType.NodeDataChanged){
                        zooKeeper.getData("/node1", this, null);
                    }
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }, null);
    }

    /**
     * 能够捕获的事件类型 :
     * NodeChildrenChanged 子节点发生变化
     * NodeDeleted 节点删除
     * @throws KeeperException
     * @throws InterruptedException
     */
    public void wathcerGetChildren() throws KeeperException, InterruptedException {
        zooKeeper.getChildren("/node1", true);
        zooKeeper.getChildren("/node1", new Watcher() {
            public void process(WatchedEvent watchedEvent) {
                System.out.println("自定义watcher");
            }
        });
    }
}

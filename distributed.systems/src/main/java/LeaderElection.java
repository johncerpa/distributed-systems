import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.quorum.Leader;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class LeaderElection implements Watcher {
    private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
    private static final int DEFAULT_PORT = 8080;
    private static final String ELECTION_NAMESPACE = "/election";
    private static final String TARGET_ZNODE = "/target_znode";
    private String currentZnodeName;
    private final OnElectionCallback onElectionCallback;

    public LeaderElection(ZooKeeper zooKeeper, OnElectionCallback onElectionCallback) {
        this.zooKeeper = zooKeeper;
        this.onElectionCallback = onElectionCallback;
    }

    /*
        Server checks communications with clients and if clients don't
        respond in `SESSION_TIMEOUT` milliseconds it will consider the client as disconnected
    */
    private static final int SESSION_TIMEOUT = 3000;

    private ZooKeeper zooKeeper;
    public void connectToZookeeper() throws IOException {
        this.zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT, this);
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        switch (watchedEvent.getType()) {
            case None:
                if (watchedEvent.getState() == Event.KeeperState.SyncConnected) {
                    System.out.println("Successfully connected to Zookeeper");
                } else {
                    synchronized (zooKeeper) {
                        System.out.println("Disconnected from Zookeeper event");
                        // wakes up all threads in wait state (in this case the main thread)
                        zooKeeper.notifyAll();
                    }
                }
                break;
            case NodeCreated:
                System.out.println(TARGET_ZNODE + " was created");
                break;
            case NodeDeleted:
                System.out.println(TARGET_ZNODE + " was deleted");
                try {
                    electLeader();
                } catch (KeeperException | InterruptedException ignored) {}
                break;
            case NodeDataChanged:
                System.out.println(TARGET_ZNODE + " data changed");
                break;
            case NodeChildrenChanged:
                System.out.println(TARGET_ZNODE + " children changed");
                break;
        }

        // After an update get all up to date data and print
        try {
            watchTargetZnode();
        } catch (KeeperException | InterruptedException ignored) {}
    }

    /*
        Puts main thread into a wait state
    */
    public void run() throws InterruptedException {
        synchronized (zooKeeper) {
            zooKeeper.wait();
        }
    }

    public void close() throws InterruptedException {
        synchronized (zooKeeper) {
            zooKeeper.close();
        }
    }

    /*
        Creates an ephemeral znode with no data in it, with no access control list
        This type of node gets deleted when it gets disconnected from Zookeeper
    */
    public void volunteerForLeadership() throws KeeperException, InterruptedException {
        String znodePrefix = ELECTION_NAMESPACE + "/c_";
        String znodeFullPath = zooKeeper.create(znodePrefix, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println("Znode name: " + znodeFullPath);
        currentZnodeName = znodeFullPath.replace(ELECTION_NAMESPACE + "/", "");
    }

    public void electLeader() throws KeeperException, InterruptedException {
        List<String> children = zooKeeper.getChildren(ELECTION_NAMESPACE, false);
        Collections.sort(children);

        String predecessorZnodeName = "";
        Stat predecessorStat = null;
        while (predecessorStat == null) {
            String smallestChild = children.get(0);
            if (smallestChild.equals(currentZnodeName)) {
                System.out.println("I am the leader");
                onElectionCallback.onElectionToBeLeader();
                return;
            } else {
                System.out.println("I am not the leader. The current leader is " + smallestChild);
                int predecessorIndex = Collections.binarySearch(children, currentZnodeName) - 1;
                predecessorZnodeName = children.get(predecessorIndex);
                /*
                    .exists() could return null because some time passes after .getChildren()
                    so seconds after the node might not exist anymore
                */
                predecessorStat = zooKeeper.exists(ELECTION_NAMESPACE + "/" + predecessorZnodeName, this);
            }
        }

        onElectionCallback.onWorker();
        System.out.println("Watching znode " + predecessorZnodeName);
    }

    /*
        Watchers that only get triggered once:
            - .exists(path, watcher)
            - .getData(path, watcher, stat)
            - .getChildren(path, watcher)
    */
    public void watchTargetZnode() throws KeeperException, InterruptedException {
        Stat stat = zooKeeper.exists(TARGET_ZNODE, this);

        if (stat == null) {
            return;
        }

        byte[] data = zooKeeper.getData(TARGET_ZNODE, this, stat);
        List<String> children = zooKeeper.getChildren(TARGET_ZNODE, this);
        System.out.println("Data: " + new String(data) + ", children: " + children);
    }

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        int currentServicePort = args.length == 1 ? Integer.parseInt(args[0]) : DEFAULT_PORT;

        ServiceRegistry serviceRegistry = new ServiceRegistry();
        OnElectionAction onElectionAction = new OnElectionAction(serviceRegistry, currentServicePort);

        LeaderElection leaderElection = new LeaderElection(serviceRegistry.getZookeeper(), onElectionAction);
        leaderElection.connectToZookeeper();

        // Leader election implementation
        leaderElection.volunteerForLeadership();
        leaderElection.electLeader();

        // Watchers and triggers
        // leaderElection.watchTargetZnode();

        leaderElection.run();
        leaderElection.close();
        System.out.println("Disconnected from Zookeeper, exiting application.");
    }
}

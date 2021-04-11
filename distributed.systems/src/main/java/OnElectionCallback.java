import org.apache.zookeeper.KeeperException;

public interface OnElectionCallback {
    void onElectionToBeLeader() throws KeeperException, InterruptedException;
    void onWorker();
}

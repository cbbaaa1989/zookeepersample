package pers.fengyitian;

import java.io.IOException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

/**   
* @Description: 
* @author lp3331  
* @date 2016年8月31日 下午5:01:55 
* @version V1.0   
*/
public class Master implements Watcher{
	
	ZooKeeper zk;
	
	String hostPort = "127.0.0.1:2181";
	
	boolean isLeader = false;
	
	String serverId = "1";
	
	public Master() throws IOException{
		zk = new ZooKeeper(hostPort, 15000, this);
	}

	@Override
	public void process(WatchedEvent e) {

		System.out.println(e);

	}
	
	void stop() throws InterruptedException{
		zk.close();
	}
	
	boolean checkMaster(){
		while(true){
			try{
				Stat stat = new Stat();
				byte[] data = zk.getData("/master", false, stat);
				isLeader = new String(data).equals(serverId);
				return true;
			}catch (KeeperException | InterruptedException e ) {

				return false;
			}
		}
	}
	/**
	 * 在zookeeper创建master数据，创建成功则获得master身份
	 */
	void runForMaster(){
		while(true){
			try {
				//创建master，成功的话则表示获取master身份成功
				zk.create("/master", serverId.getBytes(),Ids.OPEN_ACL_UNSAFE , CreateMode.EPHEMERAL);
				isLeader = true;
				
				break;
			} catch (KeeperException | InterruptedException e) {
				//创建抛出异常，则表示创建失败，获取master身份失败
				isLeader = false;
				e.printStackTrace();
			}
			if(checkMaster()) break;
		}
	}
	
	public static void main(String [] args) throws IOException, InterruptedException{
		Master master = new Master();
		
		Thread.sleep(50000);
		
		master.stop();
	}

}

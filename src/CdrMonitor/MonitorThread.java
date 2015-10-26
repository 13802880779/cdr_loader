package CdrMonitor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.kenai.jffi.Array;

import CdrConfiguration.CConf;
import CdrLogger.CLogger;
import CdrUtils.HBaseUtils;

/**
 * @author root
 *
 */


public class MonitorThread implements Runnable{

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

	private static ArrayList<MonitorObject> MOLIST;
	
	
	static{
		MOLIST=new ArrayList<MonitorObject>();
		try {
			HBaseUtils.createTableIfNotExist1(CConf.getMonitorTableName());
		} catch (Exception e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
			CLogger.logStackTrace(e);
		}
		
	}
	
	@Override
	public void run() {
		// TODO Auto-generated method stub
		while(true)
		{
			ArrayList<MonitorObject> mlcopy=null;
			try {
				Thread.sleep(CConf.getMonitorCollectInterval()*1000);	
				
				mlcopy=new ArrayList<MonitorObject>();
				//dangerous here!!, if blocked here , all kpi write action will be blocked!so just copy it
				synchronized(MOLIST){					
					mlcopy=(ArrayList<MonitorObject>)MOLIST.clone();
					MOLIST.clear();
				}
					//if >0 ,then flush to hbase table
				if(mlcopy.size()>0)
				{
					HTableInterface  ht= HBaseUtils.getConnection().getTable(CConf.getMonitorTableName());
					ArrayList<Put> list=new ArrayList<Put>();
					ht.setAutoFlush(false);
					ht.setWriteBufferSize(2*1024*1024);
					
					for(MonitorObject obj:mlcopy)
					{
						Put p=new Put(obj.getKey());
						p.add(Bytes.toBytes("C"), Bytes.toBytes("D"), obj.getValue());
						list.add(p);
					}
					ht.put(list);
					ht.close();
					list.clear();	
					list=null;
					mlcopy.clear();
					mlcopy=null;				
				
				}
				
						
				
				
			} catch (InterruptedException | IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				CLogger.logStackTrace(e);
				recover(mlcopy);
				continue;
			}
		}
	}
	
	private void recover(ArrayList<MonitorObject> al)
	{
		if(al==null)
			return;
		for(MonitorObject mo: al)
		{
			MOLIST.add(mo);
		}
	}
	
	public synchronized static void addMonitorObj(MonitorObject mo)
	{
		MOLIST.add(mo);
	}

	//private getJobQueue
	

}

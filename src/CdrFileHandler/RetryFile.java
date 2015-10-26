package CdrFileHandler;

import java.io.File;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import CdrConfiguration.CConf;
import CdrLoadHandler.CdrWorker;
import CdrLogger.CLogger;
import CdrMonitor.MonitorObject;
import CdrMonitor.MonitorThread;
import CdrParser.CdrParser;

public class RetryFile extends Thread{
	private static HashMap<String,RetryStatus> rFileList;
	private ExecutorService service;
	//private final CdrParser cp;
	
	static{
		initRetryFileList();
	}
	
	public RetryFile(ExecutorService service)
	{
		this.service=service;
		//this.cp=cp;
	}

	
	public RetryFile() {
		// TODO Auto-generated constructor stub
	}


	class RetryStatus{
		int retrycount=0;
		int status=0;//0:not running,1:running
	}
	
	public static void initRetryFileList()
	{
		if(rFileList==null)
			rFileList=new HashMap();
	}
	
	public synchronized void push2Retry(File f)
	{
		initRetryFileList();
		
		String fp=f.getAbsolutePath();
		RetryStatus rs=rFileList.get(fp);
		if(rs==null)
			rs=new RetryStatus();
		else
		{
			rs.retrycount++;
			rs.status=0;
		}
		
		if(rs.retrycount>=CConf.getJobRetryTime())
		{	
			rFileList.remove(fp);
			FileWatcher.removeFromJobQueue(fp);
		//	cp.actionAfterLoad();
			CLogger.log4j("ERROR","Retry times out, job aborted! filepath="+fp);
		}
		else	
			rFileList.put(fp, rs);	
		//rFileList.
	}
	public synchronized void removeFromRetry(File f)
	{
		if(rFileList!=null)
			rFileList.remove(f.getAbsolutePath());
	}
	public void retryJob()
	{
		initRetryFileList();
		add2Monitor();
		CLogger.log4j("INFO",FileWatcher.getJobQueueStat()+",Retry Queue Size: "+rFileList.size());
		
		
		//设置全局的WAL
		if(FileWatcher.getJobQueueSize()>CConf.getGlobalWALCloseThreshold())
		{
			CConf.isWriteToWAL(false);//关闭WAL功能
			CLogger.log4j("INFO","Job Queue Size Over "+CConf.getGlobalWALCloseThreshold()+", WAL will be closed!");
		}
		else if(FileWatcher.getJobQueueSize()<CConf.getGlobalWALReopenThreshold())
		{
			CConf.isWriteToWAL(true);//重新打开WAL功能
		//	CLogger.log4j("INFO","Job Queue Size less than "+CConf.getGlobalWALReopenThreshold()+", WAL will be reopened!");
		}
		
		Iterator iter = rFileList.entrySet().iterator();

		while (iter.hasNext()) {
			Map.Entry entry = (Map.Entry) iter.next(); 
			String fpath = (String)entry.getKey();
			RetryStatus rs = (RetryStatus)entry.getValue();
			if(rs.status==1)//already running
			{	
				//System.out.println("Job already in retry action, wait for another loop,fp="+fpath);
				continue;
			
			}
			
			rs.status=1;
			rFileList.put(fpath, rs);
			
			CdrWorker cw=new CdrWorker(new File(fpath));
			service.execute(cw);
			//System.out.println("Retry job: "+fpath+",time:"+rs.retrycount);
		}
	}
	
	public void run()
	{
		while(true)
		{
			retryJob();
			try {
				Thread.sleep(CConf.getJobRetryInterval());
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				//e.printStackTrace();
				CLogger.logStackTrace(e);
				continue;
			}
		}
	}
	
	private void add2Monitor()
	{
		long ts=System.currentTimeMillis();
		MonitorObject mo=new MonitorObject(CConf.getHostName()+"_QueueSize",ts,(long)FileWatcher.getJobQueueSize());
		MonitorThread.addMonitorObj(mo);
		mo=new MonitorObject(CConf.getHostName()+"_QueueDelay1",ts,(long)FileWatcher.getJobQueueCounter1());
		MonitorThread.addMonitorObj(mo);
		mo=new MonitorObject(CConf.getHostName()+"_QueueDelay2",ts,(long)FileWatcher.getJobQueueCounter2());
		MonitorThread.addMonitorObj(mo);		
	}

}

package CdrLoadHandler;

import java.io.File;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import CdrConfiguration.CConf;
import CdrLogger.CLogger;
import CdrMonitor.MonitorObject;
import CdrMonitor.MonitorThread;
import CdrParser.CdrParser;

public class RetryQueue extends Thread {
	private static HashMap<String, RetryStatus> rFileList;
	private static int RecoverCount = 0;
	private static int RecoverNum = 5;

	static {
		rFileList = new HashMap();
	}

	public RetryQueue() {
		// TODO Auto-generated constructor stub
	}

	private void recover() {
		RecoverCount++;
		if (RecoverCount >= RecoverNum)
			return;
		RetryQueue rq = new RetryQueue();
		rq.start();
	}

	class RetryStatus {
		int retrycount = 0;
		int status = 0;// 0:not running,1:running
	}

	public void push2Retry(File f) {
		synchronized (rFileList) {
			// initRetryFileList();

			String fp = f.getAbsolutePath();
			RetryStatus rs = rFileList.get(fp);
			if (rs == null)
				rs = new RetryStatus();
			else {
				rs.retrycount++;
				rs.status = 0;
			}

			if (rs.retrycount >= CConf.getJobRetryTime()) {
				rFileList.remove(fp);
				//JobQueue.removeFromJobQueue(fp);			
				CLogger.log4j("ERROR", "Retry times out, job aborted! filepath=" + fp);
			} else
				rFileList.put(fp, rs);
		}
	}

	public void removeFromRetry(File f) {
		synchronized (rFileList) {
			if (rFileList != null)
				rFileList.remove(f.getAbsolutePath());
		}

	}

	public void retryJob() {
		synchronized (rFileList) {

			// add2Monitor();
			// CLogger.log4j("INFO",JobQueue.getJobQueueStat()+",Retry Queue
			// Size: "+rFileList.size());
			CLogger.log4j("INFO", "Retry Queue Info: Size: " + rFileList.size());

			Iterator iter = rFileList.entrySet().iterator();

			while (iter.hasNext()) {
				Map.Entry entry = (Map.Entry) iter.next();
				String fpath = (String) entry.getKey();
				RetryStatus rs = (RetryStatus) entry.getValue();
				if (rs.status == 1)// already running
				{
					continue;
				}

				rs.status = 1;
				rFileList.put(fpath, rs);

				JobManager.SummitJob(new File(fpath));

			}
		}
	}

	public void run() {
		while (true) {
			retryJob();
			try {
				Thread.sleep(CConf.getJobRetryInterval());
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				// e.printStackTrace();
				CLogger.logStackTrace(e);
				recover();
				// continue;
			}
		}
	}

	private void add2Monitor() {
		long ts = System.currentTimeMillis();
		MonitorObject mo = new MonitorObject(CConf.getHostName(), "QueueSize", ts, (long) JobQueue.getJobQueueSize());
		MonitorThread.addMonitorObj(mo);
		for (int i = 1; i <= 5; i++) {
			mo = new MonitorObject(CConf.getHostName(), "QueueDelay" + i, ts,
					(long) JobQueue.getgetJobQueueDelay((i - 1) * 60, i * 60));
			MonitorThread.addMonitorObj(mo);
		}

		mo = new MonitorObject(CConf.getHostName(), "QueueDelay6", ts, (long) JobQueue.getJobQueueCounter2());
		MonitorThread.addMonitorObj(mo);

	}

}

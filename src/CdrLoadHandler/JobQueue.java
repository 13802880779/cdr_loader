package CdrLoadHandler;

import java.util.ArrayList;

import CdrConfiguration.CConf;
import CdrLogger.CLogger;

public class JobQueue extends Thread {
	private static ArrayList<JobQueueObj> JobQueue = new ArrayList();
	private static int RecoverCount = 0;
	private static int RecoverNum = 5;
	private boolean jswitch=false;

	public void run() {
		while (true) {

			try {
				// do something here
				CLogger.log4j("INFO", "Job Queue Info: " + getJobQueueStat());

				// 设置全局的WAL
				if (getJobQueueSize() > CConf.getGlobalWALCloseThreshold()&&!jswitch) {
					jswitch=true;
					CConf.isWriteToWAL(false);// 关闭WAL功能
					CLogger.log4j("INFO",
							"Job Queue Size Over " + CConf.getGlobalWALCloseThreshold() + ", WAL will be closed!");
				
				} else if (getJobQueueSize() < CConf.getGlobalWALReopenThreshold()&&jswitch) {
					CConf.isWriteToWAL(true);// 重新打开WAL功能
					jswitch=false;
					CLogger.log4j("INFO","Job Queue Size less than "+CConf.getGlobalWALReopenThreshold()+", WAL will be	reopened!");
				}
			

				Thread.sleep(10000);

			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				recover();
			}
		}

	}

	public void recover() {
		RecoverCount++;
		if (RecoverCount >= RecoverNum)
			return;

		JobQueue jq = new JobQueue();
		jq.start();
	}

	public static void add2JobQueue(JobQueueObj jqobj) {
		synchronized (JobQueue) {
			if(!contains(jqobj))
				JobQueue.add(jqobj);
		}
	}

	public static boolean contains(JobQueueObj obj)
	{
		for (JobQueueObj obj2:JobQueue) {
			if (obj2.JobName.equals(obj.JobName)) {
				return true;
			}
		}
		return false;
	}
	
	public static void removeFromJobQueue(String fp) {
		synchronized (JobQueue) {
			for (int i = 0; i < JobQueue.size(); i++) {
				JobQueueObj obj = JobQueue.get(i);

				if (fp.equals(obj.JobName)) {
					JobQueue.remove(i);
					i--;
					// System.out.println("remove from job queue==>"+fp);
					// break;
				}
			}
		}

	}

	public static int getJobQueueSize() {
		synchronized (JobQueue) {
			return JobQueue.size();
		}
	}

	public static int getJobQueueCounter1() {
		synchronized (JobQueue) {
			int M1Counter = 0;
			for (JobQueueObj obj : JobQueue) {
				long interval = System.currentTimeMillis() - obj.JobAddTime;
				if (interval > 60 * 1000L && interval <= 60 * 5 * 1000L)
					M1Counter++;

			}
			return M1Counter;
		}

	}

	public static int getgetJobQueueDelay(int st, int et) {
		synchronized (JobQueue) {
			int counter = 0;
			for (JobQueueObj obj : JobQueue) {
				long interval = System.currentTimeMillis() - obj.JobAddTime;
				if (interval > st * 1000L && interval <= et * 1000L)
					counter++;

			}
			return counter;
		}

	}

	public static int getJobQueueCounter2() {
		synchronized (JobQueue) {
			int M5Counter = 0;
			for (JobQueueObj obj : JobQueue) {
				long interval = System.currentTimeMillis() - obj.JobAddTime;
				if (interval > 60 * 5 * 1000L)
					M5Counter++;
			}
			return M5Counter;
		}

	}

	public static String getJobQueueStat() {
		synchronized (JobQueue) {
			int M1Counter = 0, M5Counter = 0;
			for (JobQueueObj obj : JobQueue) {
				if (System.currentTimeMillis() - obj.JobAddTime > 60 * 5 * 1000L)
					M5Counter++;
				else if (System.currentTimeMillis() - obj.JobAddTime > 60 * 1000L)
					M1Counter++;

			}
			return "Job Queue Size: " + JobQueue.size() + ", Dealy(>60s&<=300s): " + M1Counter + ", Delay(>300s): "
					+ M5Counter;
		}
	}

}

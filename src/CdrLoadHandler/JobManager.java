package CdrLoadHandler;

import java.io.File;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import CdrConfiguration.CConf;
import CdrFileHandler.FileList;
import CdrFileHandler.FileWatcher;
import CdrLogger.CLogger;
import CdrMonitor.MonitorThread;
import CdrParser.CdrParser;

public class JobManager {
	
	public static  ExecutorService service;
	public void run()
	{
		CLogger.saveAppPid();
		
	    try {
	    	//启动线程池	    
	    	service = Executors.newFixedThreadPool(CConf.getConcurrentWorkerNum());  
	       
	    	//启动文件监控线程,包括南向服务器文件监控和程序本身配置文件变动到监控
	    	FileWatcher fw=new FileWatcher();
			fw.start();
			
			//start jobqueue monitor thread
			JobQueue jq=new JobQueue();
			jq.start();
					
			
			//启动补采线程			
			RetryQueue rq=new RetryQueue();
			rq.start();
			
			
			
			//start monitor thread
			//MonitorThread mt=new MonitorThread();
			//mt.run();
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			CLogger.log4j("FATAL", "Main Thread Terminated!"+e.toString());
			CLogger.logStackTrace(e);
		}
	}
	
	public static int SummitJob(final File file)
	{
		String firm;
		CdrParser cp;
		try {
			firm = CdrParser.findFirmByFileName(file.getName());
			Class<?> c = Class.forName(CConf.getCdrParserClassName(firm));
			Constructor<?> c1 = c.getDeclaredConstructor(File.class);
			c1.setAccessible(true);
			cp = (CdrParser) c1.newInstance(file);
			int flag=cp.parse();
			if(flag==-1)
			{
				//文件解析错误，直接退出
				CLogger.log4j("ERROR","SummitJob, File parse error: "+file.getAbsolutePath());
				return -1;
			}
		} catch (Exception e) {
			//其他异常，例如reflect找不到class
			e.printStackTrace();
			CLogger.log4j("ERROR","SummitJob Exception, "+e.toString()+","+file.getAbsolutePath());
			CLogger.logStackTrace(e);
			return -1;
		}

	
		//如果文件解析成功，那么根据解析的结果，提交不同类型到jobworker
		if(cp!=null)
		{
			//System.out.println("====>"+cp.getLoadType());
			if(cp.getLoadType().equals("batch_put"))
			{
				BatchPutWorker cw = new BatchPutWorker(file,cp);
				service.execute(cw);
			}
			else if(cp.getLoadType().equals("bulk_load"))
			{
				BulkLoadWorker cw = new BulkLoadWorker(file,cp);
				service.execute(cw);
			}
		}
		
		//添加一个job到队列里面
		JobQueueObj obj = new JobQueueObj(file.getAbsolutePath(), System.currentTimeMillis());
		JobQueue.add2JobQueue(obj);
		
		return 1;
		
	}

}

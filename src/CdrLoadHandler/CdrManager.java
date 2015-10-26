package CdrLoadHandler;

import java.io.File;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import CdrConfiguration.CConf;
import CdrFileHandler.FileList;
import CdrFileHandler.FileWatcher;
import CdrFileHandler.RetryFile;
import CdrLogger.CLogger;

public class CdrManager {
	
	public static void main(String args[])
	{
		CLogger cl=new CLogger();
		CLogger.saveAppPid();
		
	    try {
	    	//启动线程池	    
	    	ExecutorService service = Executors.newFixedThreadPool(CConf.getConcurrentWorkerNum());  
	    	//service.
	    	//启动文件监控线程,包括南向服务器文件监控和程序本身配置文件变动到监控
	    	FileWatcher fw=new FileWatcher(service);
			fw.start();
			
			//启动补采线程			
			RetryFile rf=new RetryFile(service);
			rf.start();
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			CLogger.log4j("FATAL", "Main Thread exist!"+e.toString());
			CLogger.logStackTrace(e);
		}
		
	/*	while(true)
		{
			CLogger.log4j("INFO","Scanning unhandled cdr files...");
			
			FileList fl=new FileList();
			//System.out.println("==>getting unhandled file list");
			
			ArrayList<File> flist=fl.getUnHandledFilesList(CConf.getCdrSrcPath());
			
			if(flist==null)
			{
				CLogger.log4j("FATAL","Get cdr files list error, application will be terminated now!");
				break;
			}
			

			for(File f:flist)
			{
				CdrWorker cw=new CdrWorker(f);
				service.execute(cw);
			}
			try {
				Thread.sleep(CConf.getCdrMangerScanPeriod());
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				CLogger.log4j("ERROR","CdrManager.java, thread sleep,"+e.toString());
				CLogger.logStackTrace(e);

			}
		}
		service.shutdown();
		try {
			service.awaitTermination(600000, TimeUnit.MILLISECONDS);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			CLogger.logStackTrace(e);
		}
		CLogger.log4j("INFO","Application had been terminated!");
		CLogger.close();
		*/
	}
	
	public void run()
	{
		CLogger.saveAppPid();
		
	    try {
	    	//启动线程池	    
	    	ExecutorService service = Executors.newFixedThreadPool(CConf.getConcurrentWorkerNum());  
	       
	    	//启动文件监控线程,包括南向服务器文件监控和程序本身配置文件变动到监控
	    	FileWatcher fw=new FileWatcher(service);
			fw.start();
			
			//启动补采线程			
			RetryFile rf=new RetryFile(service);
			rf.start();
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			CLogger.log4j("FATAL", "Main Thread Terminated!"+e.toString());
			CLogger.logStackTrace(e);
		}
	}

}

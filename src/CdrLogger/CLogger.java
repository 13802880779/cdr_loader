package CdrLogger;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import CdrConfiguration.CConf;

public class CLogger {
	
	public static void main(String args[])
	{
		saveAppPid();
		while(true)
		{
			try {
				Thread.sleep(60000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	private static BufferedWriter LogWriter;
	private static BufferedWriter HandledFileLogWriter;
	private static Date date;
	private static Date date2;
	static
	{
		try {
			LogWriter=new BufferedWriter(new FileWriter(new File(CConf.getWorkerLogPath()),true));
			HandledFileLogWriter=new BufferedWriter(new FileWriter(new File(CConf.getProcessedFilesLogPath()),true));
			date=new Date();
			date2=new Date();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static int saveLastProcFileTS(long t)
	{
		String fn=CConf.getLastProcessed();
		if(fn!=null)
		{
			File f=new File(fn);
			try {
				BufferedWriter br=new BufferedWriter(new FileWriter(f));
				br.write(String.valueOf(t));;
				br.close();
				return 1;
			} catch (IOException e) {
				// TODO Auto-generated catch block
				log4j("ERROR","CLogger.java,"+"function: saveLastProcFileTS,"+e.toString());
				logStackTrace(e);
				e.printStackTrace();
				return -1;
			}
			
		}
		return -1;
	}
	
	public static long getLastProcFileTS()
	{
		String fn=CConf.getLastProcessed();
		if(fn!=null)
		{
			File f=new File(fn);
			try {
				BufferedReader bw=new BufferedReader(new FileReader(f));
				//BufferedWriter br;
				String line=bw.readLine();
				
				return line==null?-1L:Long.parseLong(line);
				
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				//System.out.println(e.);
				log4j("ERROR","CLogger.java,"+"function: getLastProcFileTS,"+e.getMessage());
				logStackTrace(e);
				e.printStackTrace();
				return -1L;
			}
			
		}
		return -1L;

	}
	
	/**
	 * 
	 * @param LogLevel:INFO(normal information),ERROR(Exceptions,but still works),FATAL(exceptions,and will exit)
	 * @param msg:more information about the exception
	 * example: [2015-08-01 12:12:92.212] INFO: Get File List
	 */
	
	public synchronized static void log4j(String LogLevel,String msg)
	{
		if("INFO".equals(LogLevel)&& !"INFO".equals(CConf.getLogLevel()))
			return;
		
		try {
			/*if(date.getDate()!=(new Date()).getDate())
		{	
			LogWriter.close();
			LogWriter=null;
			date=new Date();
		}
		
		//File lf=new File(CConf.getWorkerLogPath());

			if(LogWriter==null)
				LogWriter=new BufferedWriter(new FileWriter(new File(CConf.getWorkerLogPath()),true));*/
			
			LogWriter=roll2NextDayLogger(LogWriter,date,CConf.getWorkerLogPath());
			
			SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
			String datetime=format.format(new Date());	
			
			LogWriter.write("["+datetime+"] "+LogLevel+": "+msg+"\n");
			//LogWriter.close();
			LogWriter.flush();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	/*
	 * 判断当前日期与上次写入日期是否同一天，如果不是，则关闭日志文件，并生成当天日志，更新baseline将本次更新时间作为下次到baseline时间
	 */
	public static BufferedWriter roll2NextDayLogger(BufferedWriter logger,Date BaselineDate,String NextFilePath) throws IOException
	{
		if(BaselineDate.getDate()!=(new Date()).getDate())
		{	
			logger.close();
			logger=null;
			BaselineDate=new Date();
		}
		
		//File lf=new File(CConf.getWorkerLogPath());

			if(logger==null)
				logger=new BufferedWriter(new FileWriter(new File(NextFilePath),true));
			return logger;

	}
	
	
	public synchronized static void logProcessedFiles(String msg)
	{
		try {
	/*	if(date2.getDate()!=(new Date()).getDate())
		{	
			HandledFileLogWriter.close();
			HandledFileLogWriter=null;
			date2=new Date();
		}
		
		//File lf=new File(CConf.getWorkerLogPath());

			if(HandledFileLogWriter==null)
				HandledFileLogWriter=new BufferedWriter(new FileWriter(new File(CConf.getProcessedFilesLogPath()),true));*/
			
			HandledFileLogWriter=roll2NextDayLogger(HandledFileLogWriter,date2,CConf.getProcessedFilesLogPath());
			
			SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
			String datetime=format.format(new Date());	
			
			HandledFileLogWriter.write("["+datetime+"] "+msg+"\n");
			//LogWriter.close();
			HandledFileLogWriter.flush();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	
	
	public static void close()
	{
		try {
			LogWriter.close();
			HandledFileLogWriter.close();
			File f=new File(CConf.getAppPidSavedPath());
			if(f.exists()&&f.isFile())
				f.delete();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			//e.
		}
		LogWriter=null;
		HandledFileLogWriter=null;
	}
	
	public synchronized static void logStackTrace(Exception e)
	{
		StackTraceElement ste[]=e.getStackTrace();
		StringBuffer sb=new StringBuffer("");

		for(Object obj:ste)
		{
			sb.append("\t"+obj.toString()+"\n");
		}
		
		log4j("ERROR",sb.toString());

	}
	
	public static void saveAppPid()
	{
		// get name representing the running Java virtual machine.  
		String name = ManagementFactory.getRuntimeMXBean().getName();  
		String pid = name.split("@")[0];  
		BufferedWriter bw=null;
		try {
			//String f=CConf.getParamByName("")
			bw=new BufferedWriter(new FileWriter(new File(CConf.getAppPidSavedPath())));
			bw.write(pid);
			bw.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			if(bw!=null)
			{	try {
					bw.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}

	}
}

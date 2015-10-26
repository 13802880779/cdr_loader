package CdrFileHandler;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.apache.commons.io.monitor.FileAlterationListener;
import org.apache.commons.io.monitor.FileAlterationMonitor;
import org.apache.commons.io.monitor.FileAlterationObserver;

import CdrConfiguration.CConf;
import CdrLoadHandler.CdrWorker;
import CdrLogger.CLogger;
import CdrParser.CdrParser;

import java.io.File;
import java.io.FileFilter;
import java.lang.reflect.Constructor;
import java.util.concurrent.TimeUnit;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class FileWatcher {
	
	
    public static void main(String[] args) throws Exception{

        //Thread.sleep(30000);
        //monitor.stop();
    	//FileWatcher fw=new FileWatcher();
    	//fw.start();
    }
    
    private ExecutorService service;
    //保存当前所有正在被处理或等待处理到文件列表
    public static ArrayList<JobQueueObj>JobQueue=new ArrayList();
    
    public FileWatcher(ExecutorService service)
    {
    	this.service=service;
    }
    
    public void start() throws Exception
    {
        String directory[] = CConf.getCdrSrcPath();
        // 轮询间隔,默认 5 秒,这意味着如果在一个5s的监控间隙内创建并删除文件，将不会被监控到！！
        long interval = TimeUnit.SECONDS.toMillis(CConf.getFileWatchInterval());
        
        Class c = Class.forName(CConf.getCdrFileFilterClassName());    
 		FileFilter ff=(FileFilter)c.newInstance();
 		FileAlterationObserver[] observers=new FileAlterationObserver[directory.length+1];
 		for(int i=0;i<directory.length;i++)
 		{
 			System.out.println("add monitor path:"+directory[i]);
 			 observers[i] = new FileAlterationObserver(directory[i], ff);
 	        //设置文件变化监听器
 			observers[i].addListener(new CdrFileListener(this.service));
 		}
       
         
        //设置配置文件监控，每5秒检测一次，如果有改动，则重新加载所有配置信息
        String conf="conf";//configuration file path
        FileAlterationObserver ProObserver=new FileAlterationObserver(conf,FileFilterUtils.suffixFileFilter(".properties"));
        ProObserver.addListener(new PropertyFileListener());
        observers[directory.length]=ProObserver;
       // FileAlterationMonitor ProMonitor= new FileAlterationMonitor(TimeUnit.SECONDS.toMillis(5),ProObserver);
        //ProMonitor.start();
        
        FileAlterationMonitor CdrMonitor = new FileAlterationMonitor(interval,observers);
        CdrMonitor.start();
        
        
    }
    
    public synchronized static void add2JobQueue(JobQueueObj jqobj)
    {
    	JobQueue.add(jqobj);
    }
    public synchronized static void removeFromJobQueue(String fp)
    {
    	for(int i=0;i<JobQueue.size();i++)
    	{
    		JobQueueObj obj=JobQueue.get(i);
    		
    		if(fp.equals(obj.JobName))
    		{
    			JobQueue.remove(i);
    			i--;
    			//System.out.println("remove from job queue==>"+fp);
    			//break;
    		}
    	}
    	//FilesQueue.remove(fp);
    }
    public static int getJobQueueSize()
    {
    	return JobQueue.size();
    }
    public static int getJobQueueCounter1()
    {
    	int M1Counter=0;
    	for(JobQueueObj obj:JobQueue)
    	{
    		long interval=System.currentTimeMillis()-obj.JobAddTime;
    		if(interval>60*1000L && interval<=60*5*1000L)
    			M1Counter++;

    	}
    	return M1Counter;

    }
    
    public static int getJobQueueCounter2()
    {
    	int M5Counter=0;
    	for(JobQueueObj obj:JobQueue)
    	{
    		long interval=System.currentTimeMillis()-obj.JobAddTime;
    		if(interval>60*5*1000L)
    			M5Counter++;
    	}
    	return M5Counter;

    }
    
    public static String getJobQueueStat()
    {
    	int M1Counter=0,M5Counter=0;
    	for(JobQueueObj obj:JobQueue)
    	{
    		if(System.currentTimeMillis()-obj.JobAddTime>60*5*1000L)
    			M5Counter++;
    		else if(System.currentTimeMillis()-obj.JobAddTime>60*1000L)
    			M1Counter++;

    	}
    	return "Job Queue Size: "+JobQueue.size()+", Dealy(>60s&<=300s): "+M1Counter+", Delay(>300s): "+M5Counter;
    }
}


final class JobQueueObj{
	public String JobName;
	public long JobAddTime;
	public JobQueueObj(String jn,long jt)
	{
		this.JobAddTime=jt;
		this.JobName=jn;
	}
}


final class PropertyFileListener implements FileAlterationListener
{

	@Override
	public void onDirectoryChange(File arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onDirectoryCreate(File arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onDirectoryDelete(File arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onFileChange(File arg0) {
		// TODO Auto-generated method stub
		System.out.println("property file changed!");
		CConf.readParams();
		//System.out.println(CConf.getCdrSrcPath());
		CLogger.log4j("INFO","CConf, job.properties updated!");
		
	}

	@Override
	public void onFileCreate(File arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onFileDelete(File arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onStart(FileAlterationObserver arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onStop(FileAlterationObserver arg0) {
		// TODO Auto-generated method stub
		
	}
	
}

final class CdrFileListener implements FileAlterationListener{
	private ExecutorService service;
	
	
	public CdrFileListener(ExecutorService service)
	{
		this.service=service;
	}
    @Override
    public void onStart(FileAlterationObserver fileAlterationObserver) {
    //    System.out.println("monitor start scan files..");
    }


    @Override
    public void onDirectoryCreate(File file) {
       // System.out.println(file.getName()+" directory created.");
    }


    @Override
    public void onDirectoryChange(File file) {
       // System.out.println(file.getName()+" directory changed.");
    }


    @Override
    public void onDirectoryDelete(File file) {
       // System.out.println(file.getName()+" directory deleted.");
    }


    @Override
    public void onFileCreate(File file) {
       System.out.println("File Created: "+file.getAbsolutePath());
       JobQueueObj obj=new JobQueueObj(file.getAbsolutePath(),System.currentTimeMillis());
       FileWatcher.add2JobQueue(obj);
       CdrWorker cw=new CdrWorker(file);
       service.execute(cw);
    }


    @Override
    public void onFileChange(File file) {
        System.out.println("File Changed: "+file.getAbsolutePath());
    }


    @Override
    public void onFileDelete(File file) {
        System.out.println("File Deleted: "+file.getAbsolutePath());
        //System.out.println("File Parent:"+file.getParent());
    }


    @Override
    public void onStop(FileAlterationObserver fileAlterationObserver) {
    //    System.out.println("monitor stop scanning..");
    }
}


package CdrFileHandler;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Calendar;

import CdrConfiguration.CConf;
import CdrLogger.CLogger;
/**
 * 
 * @author root
 * @version 0.1
 * usage:
 * 1、从定义到源数据目录中，读取上次至今未处理到所有文件列表，通过lastmodified来判断，源数据目录
 * 中文件修改时间大于上个处理周期记录的最大到lastmodified的所有文件；
 * 2、根据输入到处理线程数量，对未处理到文件列表进行分组（目前只进行简单分组，未来可扩展为尽量保证每组数据量一致的算法)
 * 3、如需实现其他的判断算法，只需重写getUnHandledFilesList方法
 *
 */


public class FileList {
	
	public static void main(String[] args)
	{
		FileList fl=new FileList();
		fl.showFileLists();
	}
	
	private ArrayList<File> FL=new ArrayList();
	private long LastFileTS=-1L;
	private static long CALLCOUNTER=-1L;
	
	public void showFileLists()
	{
		String cdr_path=CConf.getCdrSrcPath()[0];
		ArrayList al=getUnHandledFilesList(cdr_path);
		if(al==null)
			return;
		ArrayList als[]=splitFilesList(al,10);
		for(int i=0; i<als.length; i++)
		{
			CLogger.log4j("INFO","array===>"+i+"==>"+als[i].size());
			ArrayList<File> alist=(ArrayList)als[i];
			for(File f:alist)
			{
				System.out.println(f.getAbsolutePath()+"_"+f.lastModified());
			}
			
		}
		
		saveLastProcFileTS();
		CLogger.close();
	}
	
/**
 * 
 * @return 
 */
	
	public long getLastProcFileTS()
	{
		return LastFileTS;
		
	}

	public ArrayList<File> getUnHandledFilesList(String desc)
	{
		
		//ArrayList al=new ArrayList();
		CALLCOUNTER++;
		
		//if(FL==null)
		FL.clear();;
		LastFileTS=CLogger.getLastProcFileTS();
		
		//如果第一次是-1，那么遍历整个文件夹到数据，之后如果还是-1，那么代表保存上次修改时间出了问题，为了避免程序重新遍历所有
		//文件，这里取当前时间-程序的重复遍历间隔时间作为上次修改时间
		if(LastFileTS==-1&&CALLCOUNTER>0)
		{
			//System.out.println("LastFileTS=System.currentTimeMillis()-CConf.getCdrMangerScanPeriod()");
			LastFileTS=System.currentTimeMillis()-CConf.getCdrMangerScanPeriod();
		}
		
		File f;
		if(desc==null||!(f=new File(desc)).isDirectory())//we want a folder to scan
		{
			CLogger.log4j("FATAL", "Cdr dest path: \""+desc+"\" is not a valid folder, please check your configuration!");
			return null;
		}
		
		getAllUnHandledFiles(FL,f,LastFileTS);
		
		//保存最新到文件修改时间
		saveLastProcFileTS();
		
		return FL;
		
	}
	
	/**
	 * 更新保存最近文件的时间戳
	 */
	public void saveLastProcFileTS()
	{
		CLogger.saveLastProcFileTS(this.getLastProcFileTS());
	}
/**
 * 
 * @param al 保存所有未处理到文件列表
 * @param f 源文件目录到file对象
 * @param lastHandledTime 时间戳
 * 返回所有大于时间戳的未处理到文件
 */
	private  void getAllUnHandledFiles(ArrayList al,File f,long lastHandledTime)
	{

		for(File f1:f.listFiles())
		{
			if(f1.isDirectory())
			{
				getAllUnHandledFiles(al,f1,lastHandledTime);
			}
			else
			{
				if(!isValidCdrType(f1))
					continue;
				if(f1.lastModified()>lastHandledTime)
				{
					al.add(f1);
					//System.out.println("here");
					if(f1.lastModified()>this.LastFileTS)
					{
						//System.out.println(f1.lastModified());
						this.LastFileTS=f1.lastModified();
					}
				}
				
			}
		}
		
		

	}
	
	private boolean isValidCdrType(File f)
	{
		String[] ValidCdrTypes=CConf.getCdrFileSuffix();
		for(String s:ValidCdrTypes)
		{
			if(f.getName().endsWith("."+s))
				return true;
		}
		return false;
	}
	
	private ArrayList[] splitFilesList(ArrayList fl, int numSplit)
	{
		if(numSplit<=0)
			return null;
		ArrayList al[];
		if(fl.size()<numSplit)
			al=new ArrayList[fl.size()];
		else
			al=new ArrayList[numSplit];
		for(int i=0; i<al.length;i++)
			al[i]=new ArrayList();
		
		//System.out.println(fl.size());
		int count=fl.size()/numSplit;
		

		for(int j=0; j<al.length; j++)
		{	
			//System.out.println("j==>"+j);
			for(int i=0;i<count; i++)
			{
				al[j].add(fl.get(j*count+i));
			}
		}
		int count2=fl.size()%numSplit;
		for(int i=0;i<count2; i++)
		{
			al[i].add(fl.get(numSplit*count+i));
		}
		return al;
	}
	

}

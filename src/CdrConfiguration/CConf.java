package CdrConfiguration;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;

import org.apache.hadoop.hbase.client.Durability;

import CdrLogger.CLogger;

public class CConf {

	private static String conf_path="conf/job.properties";
	private static HashMap params=new HashMap();
	private static boolean isWriteToWAL;
	private static String hostname=null;
	static{
		isWriteToWAL=true;
		readParams();
	}
	
	public static void readParams() 
	{
		
		HashMap tmpMap=new HashMap();
		try {
			BufferedReader br=new BufferedReader(new FileReader(conf_path));
			String line="";
			while((line=br.readLine())!=null)
			{
				if(line.startsWith("#")||line.startsWith("//"))
					continue;
				
				int pos=line.indexOf("=");
				if(pos!=-1){
				String paramKey=line.substring(0, pos).trim();
				String paramValue=line.substring(pos+1, line.length()).trim();
				tmpMap.put(paramKey, paramValue);
				}
			}
			br.close();			
			params.clear();
			params=tmpMap;			
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
		//	CLogger.log4j("ERROR", "CConf, setup or update job.properties failed!"+e.toString());
			e.printStackTrace();
			tmpMap.clear();
			tmpMap=null;
		}
		
	}
	
	public static void main(String args[])
	{
		System.out.println("getCdrSrcPath "+CConf.getCdrSrcPath());
		System.out.println("getAppPidSavedPath "+CConf.getAppPidSavedPath());
		System.out.println("getProcessedFilesLogPath "+CConf.getProcessedFilesLogPath());
		System.out.println("getWorkerLogPath "+CConf.getWorkerLogPath());
		System.out.println("getLogLevel "+CConf.getLogLevel());
		System.out.println("getCdrHTableName "+CConf.getCdrHTableName("hw","aiu-ho"));
		System.out.println("getCdrFirms "+CConf.getCdrFirms());
		System.out.println("getCdrPrefix "+CConf.getCdrPrefix("hw"));
		System.out.println("getJobRetryTime "+CConf.getJobRetryTime());
		System.out.println("getJobRetryInterval "+CConf.getJobRetryInterval());
		System.out.println("getCdrParserClassName "+CConf.getCdrParserClassName("hw"));
		System.out.println("getCdrRegionNum "+CConf.getCdrRegionNum("hw","aiu-mm"));
		System.out.println("getCdrDelim "+CConf.getCdrDelim("hw","aiu-mm"));
		System.out.println("getCdrPrimaryKeyIdx "+CConf.getCdrPrimaryKeyIdx("hw","aiu-mm"));
		System.out.println("getCdrTimeStampIdx "+CConf.getCdrTimeStampIdx("hw","aiu-mm"));
		System.out.println("getCdrColumnCount "+CConf.getCdrColumnCount("hw","aiu-mm"));
		System.out.println("getCdrXidIndex "+CConf.getCdrXidIndex("hw","aiu-mm"));
		System.out.println("getCdrIndexConfiguration "+CConf.getCdrIndexConfiguration("hw","aiu-mm"));
		System.out.println("getCdrIndexRegionNum:"+CConf.getCdrIndexRegionNum("hw","aiu-mm"));
		
		System.out.println("getFileWatchInterval "+CConf.getFileWatchInterval());
		System.out.println("getAppMetaTableName "+CConf.getAppMetaTableName());
		System.out.println("getConcurrentWorkerNum "+CConf.getConcurrentWorkerNum());		
		System.out.println("getHtableTTL "+CConf.getHtableTTL());		
		
		System.out.println("HostName: "+getHostName());

		
	}
	
	public static String getLinuxHostName() {
		try {
			return (InetAddress.getLocalHost()).getHostName();
		} catch (UnknownHostException uhe) {
			String host = uhe.getMessage(); // host = "hostname: hostname"
			if (host != null) {
				int colon = host.indexOf(':');
				if (colon > 0) {
					return host.substring(0, colon);
				}
			}
			return "UnknownHost";
		}
	}


	public static String getHostName() {
		if(hostname!=null)
			return hostname;
		
		if (System.getenv("COMPUTERNAME") != null) {
			hostname= System.getenv("COMPUTERNAME");
			
		} else {
			hostname= getLinuxHostName();
		}
		return hostname;
	}
	
	public static String getMonitorTableName()
	{
		return CConf.getParamByName("app.monitor.htable");		
	}
	public static int getMonitorCollectInterval()
	{
		return CConf.getParamByName("app.monitor.collect.interval")==null?10:Integer.parseInt(CConf.getParamByName("app.monitor.collect.interval"));		
	}

	public static void isWriteToWAL(boolean isw2wal)
	{
		CConf.isWriteToWAL=isw2wal;
	}
	
	public static String[] getCdrSrcPath()
	{
		return CConf.getParamByName("cdr.file.src.path").split(",");
	}
	
	public static String getParamByName(String pname)
	{
		return params.get(pname)==null?null:(String)params.get(pname);
	}
	public static String getLastProcessed()
	{
		return CConf.getParamByName("cdr.file.lastprocessed.path")==null?"lpts":CConf.getParamByName("cdr.file.lastprocessed.path");
	}
	

	public static String getAppPidSavedPath()
	{
		return CConf.getParamByName("app.pid.saved.path")==null?"pid":CConf.getParamByName("app.pid.saved.path");
	}
	public static String getWorkerLogPath()
	{
		return CConf.getParamByName("app.worker.log")!=null?formatLogName(CConf.getParamByName("app.worker.log")):"worker_log.log";
	}
	public static String getProcessedFilesLogPath()
	{
		return CConf.getParamByName("app.processed.files.log")!=null?formatLogName(CConf.getParamByName("app.processed.files.log")):"handled_files.log";

	}
	
	public static String getLogLevel()
	{
		return CConf.getParamByName("app.log.level")==null?"INFO":CConf.getParamByName("app.log.level");
	}
	public static String[] getCdrFileSuffix()
	{
		return CConf.getParamByName("cdr.file.suffix")==null?null:CConf.getParamByName("cdr.file.suffix").split(",");
	}
	public static String getCdrHTableName(String cdrFirm,String cdrType)
	{
		//System.out.println("cdr.file."+cdrFirm+"."+cdrType+".target.tablename");
		return CConf.getParamByName("cdr."+cdrFirm+"."+cdrType+".target.table");
		
	}
	public static String getCdrFileFilterClassName()
	{
		return CConf.getParamByName("cdr.file.filter.classname");
	}
	public static String[] getCdrFirms()
	{
		return CConf.getParamByName("cdr.firms")==null?null:CConf.getParamByName("cdr.firms").split(",");
	}
	public static String[] getCdrPrefix(String cdrFirm)
	{
		//System.out.println("cdr.file."+cdrFirm+".prefix");
		return CConf.getParamByName("cdr."+cdrFirm+".prefix")==null?null:CConf.getParamByName("cdr."+cdrFirm+".prefix").split(",");
	}
	public static int getJobRetryTime()
	{
		return CConf.getParamByName("app.job.retry.times")==null?3:Integer.parseInt(CConf.getParamByName("app.job.retry.times"));
	}
	public static int getJobRetryInterval()
	{
		return CConf.getParamByName("app.job.retry.interval")==null?60000:Integer.parseInt(CConf.getParamByName("app.job.retry.interval"));
	}
	
	public static String getCdrParserClassName(String firm)
	{
		return CConf.getParamByName("cdr."+firm+".parser.classname");
	}

	public static int getCdrRegionNum(String firm,String tPrefix)
	{
		return CConf.getParamByName("cdr."+firm+"."+tPrefix+".pre-build.region.num")==null?10:Integer.parseInt(CConf.getParamByName("cdr."+firm+"."+tPrefix+".pre-build.region.num"));
	}
	
	public static String getCdrDelim(String firm,String tPrefix)
	{
		return CConf.getParamByName("cdr."+firm+"."+tPrefix+".delim")==null?",":CConf.getParamByName("cdr."+firm+"."+tPrefix+".delim");
	}
	public static int getCdrPrimaryKeyIdx(String firm,String tPrefix)
	{
		return CConf.getParamByName("cdr."+firm+"."+tPrefix+".msisdn.index")==null?-1:Integer.parseInt(CConf.getParamByName("cdr."+firm+"."+tPrefix+".msisdn.index"));

	}
	public static int getCdrTimeStampIdx(String firm,String tPrefix)
	{
		return CConf.getParamByName("cdr."+firm+"."+tPrefix+".timestamp.index")==null?-1:Integer.parseInt(CConf.getParamByName("cdr."+firm+"."+tPrefix+".timestamp.index"));

	}
	public static int getCdrColumnCount(String firm,String tPrefix)
	{
		return CConf.getParamByName("cdr."+firm+"."+tPrefix+".column.num")==null?-1:Integer.parseInt(CConf.getParamByName("cdr."+firm+"."+tPrefix+".column.num"));

	}
	//文件扫描间隔
	public static int getFileWatchInterval()
	{
		return CConf.getParamByName("app.file.watch.interval")==null?5:Integer.parseInt(CConf.getParamByName("app.file.watch.interval"));

	}
	public static String getAppMetaTableName()
	{
		return CConf.getParamByName("app.metadata.htable")==null?"cdrloader_metadata":CConf.getParamByName("app.metadata.htable");
	}
	
	public static int getConcurrentWorkerNum()
	{
		return CConf.getParamByName("app.concurrent.worker")==null?20:Integer.parseInt(CConf.getParamByName("app.concurrent.worker"));
	}
	public static int getCdrXidIndex(String firm,String tPrefix)
	{
		return CConf.getParamByName("cdr."+firm+"."+tPrefix+".xdrid.index")==null?-1:Integer.parseInt(CConf.getParamByName("cdr."+firm+"."+tPrefix+".xdrid.index"));
	}
	
	public static Durability getHtableDurability(String firm,String tPrefix)
	{
		//首先判断全局到WAL设置，如果全局设置为false，那么所有表的WAL都默认为false，否则再根据每个表到具体设置进行判断
		if(!CConf.isWriteToWAL)
			return Durability.SKIP_WAL;
		
		String wal=CConf.getParamByName("cdr."+firm+"."+tPrefix+".htable.put.durability");
		if(wal==null)
		{	
			//System.out.println("null=====>SYNC_WAL");
			return Durability.SYNC_WAL;		
		}
		switch(wal)
		{
		case "ASYNC_WAL":
			return Durability.ASYNC_WAL;
		case "FSYNC_WAL":
			return Durability.FSYNC_WAL;
		case "SKIP_WAL":
			//System.out.println("=====>SKIP_WAL");
			return Durability.SKIP_WAL;
		default:
			//System.out.println("=====>SYNC_WAL");
			return Durability.SYNC_WAL;			
		}
		
		
		//return CConf.getParamByName("cdr."+firm+"."+tPrefix+".xdrid.index")==null?-1:Integer.parseInt(CConf.getParamByName("cdr."+firm+"."+tPrefix+".xdrid.index"));
	}
	public static int getGlobalWALCloseThreshold()
	{
		return CConf.getParamByName("app.global.wal.close.threshold")==null?2000:Integer.parseInt(CConf.getParamByName("app.global.wal.close.threshold"));

	}
	public static int getGlobalWALReopenThreshold()
	{
		return CConf.getParamByName("app.global.wal.reopen.threshold")==null?200:Integer.parseInt(CConf.getParamByName("app.global.wal.reopen.threshold"));

	}
	//unused
	public static long getCdrMangerScanPeriod()
	{
		return CConf.getParamByName("cdr.scan.period")==null?60000L:Long.parseLong(CConf.getParamByName("cdr.scan.period"));
		
	}
	public static int getCdrTimeStampIdx(String firm,String prefix,int idx)
	{
		//System.out.println("cdr."+firm+"."+prefix+".ts"+idx+"idx.index");
		return CConf.getParamByName("cdr."+firm+"."+prefix+".ts"+idx+"idx.index")==null?-1:Integer.parseInt(CConf.getParamByName("cdr."+firm+"."+prefix+".ts"+idx+"idx.index"));
	}
	public static int getHtableTTL()
	{
		return CConf.getParamByName("cdr.htable.ttl")==null?2592000:Integer.parseInt(CConf.getParamByName("cdr.htable.ttl"));
	}
	
	public static long getHtableWriteBufferSize()
	{
		return CConf.getParamByName("app.htable.write.buffer.size")==null?8*1024*1024L:Long.parseLong(CConf.getParamByName("app.htable.write.buffer.size"));
	}
	
	public static String getPath2EgnoreSuffix(String firm)
	{
		return CConf.getParamByName("cdr."+firm+".path2egnore.suffix")==null?"":CConf.getParamByName("cdr."+firm+".path2egnore.suffix");
	}
	public static String getPath2HandleSuffix(String firm)
	{
		return CConf.getParamByName("cdr."+firm+".path2handle.suffix")==null?"":CConf.getParamByName("cdr."+firm+".path2handle.suffix");
	}
	
	public static String getCdrIndexConfiguration(String firm,String prefix)
	{
		//System.out.println("cdr."+firm+"."+prefix+".secondary.index");
		return CConf.getParamByName("cdr."+firm+"."+prefix+".secondary.index");
	}
	
	public static int getCdrIndexRegionNum(String firm,String prefix)
	{
		return CConf.getParamByName("cdr."+firm+"."+prefix+".secondary.index.pre-build.region.num")==null?10:Integer.parseInt(CConf.getParamByName("cdr."+firm+"."+prefix+".secondary.index.pre-build.region.num"));
	}

	public static String getActionAfterLoad(String firm)
	{
		return CConf.getParamByName("cdr."+firm+".action.afterload");
	}
	public static String getMoveToPathPrefix(String firm)
	{
		return CConf.getParamByName("cdr."+firm+".desc.path.startwith");
	}
	public static String getFromPathPrefix(String firm)
	{
		return CConf.getParamByName("cdr."+firm+".src.path.startwith");
	}
	public static int getHtableBatchPutSize()
	{
		return CConf.getParamByName("app.htable.batch.put.size")==null?1000:Integer.parseInt(CConf.getParamByName("app.htable.batch.put.size"));
	}
	
	
	public static String formatLogName(String s)
	{
		Calendar c=Calendar.getInstance();
		//String month=;
		String ss=new String(s);
		ss=ss.replaceAll("%Y",""+c.get(Calendar.YEAR)+"");
		ss=ss.replaceAll("%M", c.get(Calendar.MONTH)<9?"0"+(c.get(Calendar.MONTH)+1):""+(c.get(Calendar.MONTH)+1));
		ss=ss.replaceAll("%D", c.get(Calendar.DATE)<10?"0"+c.get(Calendar.DATE):""+c.get(Calendar.DATE));
		return ss;
	}
	
}

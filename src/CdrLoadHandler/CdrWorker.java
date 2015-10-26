package CdrLoadHandler;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import CdrConfiguration.CConf;
import CdrFileHandler.FileWatcher;
import CdrFileHandler.RetryFile;
import CdrLogger.CLogger;
import CdrMonitor.MonitorObject;
import CdrMonitor.MonitorThread;
import CdrParser.CdrParser;
import CdrParser.CdrIndexParser;
import CdrUtils.HBaseUtils;
import CdrUtils.StrUtils;

public class CdrWorker implements Runnable {
	
	public static void main(String args[])
	{
		File f=new File("pid");
		try {
			Thread.sleep(30000);
			System.out.println(f.getName());
			if(!f.exists())
				System.out.println(f.getAbsolutePath()+" is deleted!");
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	//	CdrWorker cw=new CdrWorker(f);
	//	cw.run();
	} 

	File F=null;
	
	private long fileSize=0L;
	private long rowsCount=0L;
	private long skippedRowsCount=0L;
	private String status="RUNNING";
	private CdrParser cp;
	private long starttime=0L;
	private long endtime=0L;
	private long endtime2=0L;
	private int loadSpeed=0;
	private long indexCount=0L;
	
	
	public CdrWorker(File f)
	{
		this.F=f;
		
	}
	
	public long getFileSize()
	{
		return F.length();	
	}
	
	public long getRowsCount()
	{
		return rowsCount;
	}
	
	@Override
	public void run() {
		// TODO Auto-generated method stub
		
		this.starttime=System.currentTimeMillis();
		//parse file->file date,htable name,rowkey...
		CLogger.log4j("INFO", "Worker Started, Processing: "+F.getAbsolutePath());
		
		RetryFile rf=new RetryFile();
		
		//after a long wait, the file may be deleted or move!
		if(!F.exists())
		{
			//文件被删除或者被移动，直接退出
			FileWatcher.removeFromJobQueue(F.getAbsolutePath());
			CLogger.log4j("ERROR","Worker Terminated, File Not Found: "+F.getAbsolutePath());
			this.status="FILE_NOT_FOUND";
			rf.push2Retry(F);//put to retry file list
			return;
		}
		try {		
		String firm=CdrParser.findFirmByFileName(F.getName());
		//如果直接在findFirmByFileName方法中抛出异常，那就没这么麻烦了！
		//if(firm==null)
		//{
		//	CLogger.log4j("ERROR","Worker thread, Can not determine cdr firms, skip: "+F.getAbsolutePath());
		//	this.status="FILE_SKIPPED";
		//	return;
		//}
		Class<?> c;

			c = Class.forName(CConf.getCdrParserClassName(firm));
			Constructor<?> c1=c.getDeclaredConstructor(File.class);
			c1.setAccessible(true);
			cp=(CdrParser)c1.newInstance(F);
			
			int flag=cp.parse();
			if(flag==-1)
			{
				FileWatcher.removeFromJobQueue(F.getAbsolutePath());
				CLogger.log4j("ERROR","Worker Terminated, File parse error: "+F.getAbsolutePath());
				this.status="FILE_PARSE_ERROR";
				return;
			}
			
			//创建hbase表，并将表元数据信息保存到cdr_metadata表中
			HBaseUtils.createTableIfNotExist(cp);
			
			HTableInterface ht=HBaseUtils.getConnection().getTable(cp.getCdrTargetHTable());			
			ht.setAutoFlush(false);
			ht.setWriteBufferSize(CConf.getHtableWriteBufferSize());
			
			int BATCH_SIZE=CConf.getHtableBatchPutSize();			
			List<Put> pl = new ArrayList<Put>();
			HTableInterface idx_ht=null;
			if(cp.isSecondaryIndex())
			{
				idx_ht=HBaseUtils.getConnection().getTable(cp.getCdrIndexHTable());
				idx_ht.setAutoFlush(false);
				idx_ht.setWriteBufferSize(CConf.getHtableWriteBufferSize());
			}
			
			//创建二级索引存储对象			
			List<Put>si_pl=new ArrayList<Put>();

			BufferedReader br = new BufferedReader(new FileReader(F));
			String row;
			while ((row = br.readLine()) != null) {
				
				this.rowsCount++;
				
				//do something here when exception happened!
				String r[]=StrUtils.split(row, cp.getDelim());
				
				byte[] rk=cp.generatetRowKey(r);				
				if(rk==null){
					this.skippedRowsCount++;	
					//System.out.println(row);
					//CLogger.log4j("INFO",row);
					continue;
				}
				
				Put p = new Put(rk);
				p.add(Bytes.toBytes("C"), Bytes.toBytes("D"),//comlumn family  'cf' for default, column name 'Q'
						Bytes.toBytes(row));
				p.setDurability(cp.getHtableDurability());;
				pl.add(p);			
				
				if (pl.size() > BATCH_SIZE) {
					ht.put(pl);
					//ht.flushCommits();
					pl.clear();
				}	
				
				
				if(cp.isSecondaryIndex())
				{
					ArrayList<byte[]> rks=cp.generateIndexRowkey(r, cp.getSecondaryIndexPreBuildRNum(), rk);
					if(rks!=null)
					{
						for(byte[] srk:rks)
						{
							Put si_p=new Put(srk);
							si_p.add(Bytes.toBytes("C"), Bytes.toBytes("D"),//comlumn family  'cf' for default, column name 'Q'
									Bytes.toBytes("")); //value为空，所有信息都包含在rowkey里面了
							si_p.setDurability(cp.getHtableDurability());;
							si_pl.add(si_p);
							this.indexCount++;
							
						}
					}
					
					if(si_pl.size()>BATCH_SIZE)
					{
						idx_ht.put(si_pl);
						si_pl.clear();
					}
				}
				
			}
			if (pl.size() > 0) {
				ht.put(pl);
				pl.clear();
			}
			
			if(cp.isSecondaryIndex()&&si_pl.size()>0)
			{
				idx_ht.put(si_pl);
				pl.clear();
				idx_ht.close();
			//	this.endtime2=System.currentTimeMillis();
			}
			
			ht.close();
			br.close();
			
			this.endtime=System.currentTimeMillis();
			
			//保存二级索引
			/*if(si_pl.size()!=0){			
			HTableInterface si_htable=HBaseUtils.getConnection().getTable(cp.getCdrIndexHTable());
			si_htable.setAutoFlush(false);
			si_htable.setWriteBufferSize(1024*1024*8);
			si_htable.put(si_pl);
			si_pl.clear();
			si_htable.close();
			this.endtime2=System.currentTimeMillis();
			}*/

		} catch (Exception  e) {
			// TODO Auto-generated catch block
			this.status="FAILED";
			e.printStackTrace();
			CLogger.log4j("ERROR","Worker thread Exception, "+e.toString()+","+F.getAbsolutePath());
			CLogger.logStackTrace(e);
			
			//可能是网络、hbase集群等问题导致到异常，将文件加入到重试列表
			rf.push2Retry(F);
			return;			
		}
		
		add2Monitor();
		
		this.status="SUCCESSED";		
		rf.removeFromRetry(F);
		FileWatcher.removeFromJobQueue(F.getAbsolutePath());
		CLogger.logProcessedFiles(getTaskStat());
		

	}
	public long getUsedTime()
	{
		return this.endtime-this.starttime;
	}
	public int getLoadSpeed()
	{
		return (int)((this.rowsCount+this.indexCount)*1000/this.getUsedTime());
	}
	public long getSkippedRowsCount()
	{
		return this.skippedRowsCount;
	}
	public long getIndexRowCount()
	{
		return this.indexCount;
	}
	public long getIndexTotalSize()
	{
		return this.indexCount*40;
	}

	private String getTaskStat()
	{
		String s;
		if(cp!=null)
			s="File Name:"+cp.getCdrName()+",Firm:"+cp.getCdrFirm()+",Target Table:"+cp.getCdrTargetHTable()+",";
		else
			s="File Name:"+this.F.getName()+"\n";
		
		s+="Status:"+this.status+",File Size:"+this.getFileSize()+" bytes,TotalRowsCount(data+index):"
		+(this.getRowsCount()+this.getIndexRowCount())+",SkippedRowsCount:"+this.getSkippedRowsCount()
		+",Used Time:"+this.getUsedTime()+" ms, Load Speed: "+this.getLoadSpeed()+" rows/s"
		+",IndexRowCount:"+this.getIndexRowCount()+",IndexTotalSize:"+this.getIndexTotalSize()+" bytes";		
		return s;
	}
	//
	private void add2Monitor()
	{
		long ts=System.currentTimeMillis();
		MonitorObject mo=new MonitorObject(cp.getCdrTargetHTable()+"_rc",ts,this.getRowsCount());
		MonitorThread.addMonitorObj(mo);
		mo=new MonitorObject(cp.getCdrIndexHTable()+"_irc",ts,this.getIndexRowCount());
		MonitorThread.addMonitorObj(mo);
		mo=new MonitorObject(cp.getCdrTargetHTable()+"_src",ts,this.getSkippedRowsCount());
		MonitorThread.addMonitorObj(mo);
		mo=new MonitorObject(cp.getCdrTargetHTable()+"_fs",ts,this.getFileSize());
		MonitorThread.addMonitorObj(mo);
		mo=new MonitorObject(cp.getCdrTargetHTable()+"_ut",ts,this.getUsedTime());
		MonitorThread.addMonitorObj(mo);

	}

}

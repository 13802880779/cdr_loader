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
import CdrLogger.CLogger;
import CdrMonitor.MonitorObject;
import CdrMonitor.MonitorThread;
import CdrParser.CdrParser;
import CdrParser.CdrIndexParser;
import CdrUtils.HBaseUtils;
import CdrUtils.StrUtils;

public class BatchPutWorker implements Runnable {
	
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
	private CdrParser cp=null;
	private long starttime=0L;
	private long endtime=0L;
	private long endtime2=0L;
	private int loadSpeed=0;
	private long indexCount=0L;
	
	
//	public BatchPutWorker(File f)
	//{
	//	this.F=f;
		
//	}
	public BatchPutWorker(File f,CdrParser p)
	{
		this.F=f;
		this.cp=p;
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
		
		RetryQueue retryQueue=new RetryQueue();
		try {	
		//after a long wait, the file may be deleted or move!
		if(!F.exists())
		{
			//文件被删除或者被移动，直接退出
			//JobQueue.removeFromJobQueue(F.getAbsolutePath());
			CLogger.log4j("ERROR","Worker Terminated, File Not Found: "+F.getAbsolutePath());
			this.status="FILE_NOT_FOUND";
			retryQueue.removeFromRetry(F);
			return;
		}


			/*String firm=CdrParser.findFirmByFileName(F.getName());
			Class<?> c = Class.forName(CConf.getCdrParserClassName(firm));
			Constructor<?> c1=c.getDeclaredConstructor(File.class);
			c1.setAccessible(true);
			cp=(CdrParser)c1.newInstance(F);
			
			int flag=cp.parse();
			if(flag==-1)
			{
				JobQueue.removeFromJobQueue(F.getAbsolutePath());
				CLogger.log4j("ERROR","Worker Terminated, File parse error: "+F.getAbsolutePath());
				this.status="FILE_PARSE_ERROR";
				return;
			}*/
			
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

					continue;
				}
				
				Put p = new Put(rk);
				p.add(Bytes.toBytes("C"), Bytes.toBytes("D"),//comlumn family  'cf' for default, column name 'Q'
						Bytes.toBytes(row));
				p.setDurability(cp.getHtableDurability());;
				pl.add(p);			
				
				if (pl.size() > BATCH_SIZE) {
					ht.put(pl);
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
			
			this.status="SUCCESSED";		
			//JobQueue.removeFromJobQueue(F.getAbsolutePath());	//move to finally	
			//may this job comes from retry queue, so delete it
			retryQueue.removeFromRetry(F);
			CLogger.logProcessedFiles(getTaskStat());

		} catch (Exception  e) {
			// TODO Auto-generated catch block
			this.status="FAILED";
			e.printStackTrace();
			CLogger.log4j("ERROR","Worker thread Exception, "+e.toString()+","+F.getAbsolutePath());
			CLogger.logStackTrace(e);
			
			//JobQueue.removeFromJobQueue(F.getAbsolutePath());//move to finally
			//可能是网络、hbase集群等问题导致到异常，将文件加入到重试列表
			retryQueue.push2Retry(F);
			//return;			
		}finally{
			JobQueue.removeFromJobQueue(F.getAbsolutePath());
		}

		

		

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
	/*	MonitorObject mo=new MonitorObject(cp.getCdrTargetHTable(),"RowsCount",ts,this.getRowsCount());
		MonitorThread.addMonitorObj(mo);
		mo=new MonitorObject(cp.getCdrIndexHTable(),"IndexRowsCount",ts,this.getIndexRowCount());
		MonitorThread.addMonitorObj(mo);
		mo=new MonitorObject(cp.getCdrTargetHTable(),"SkippedRowsCount",ts,this.getSkippedRowsCount());
		MonitorThread.addMonitorObj(mo);
		mo=new MonitorObject(cp.getCdrTargetHTable(),"FileSize",ts,this.getFileSize());
		MonitorThread.addMonitorObj(mo);
		mo=new MonitorObject(cp.getCdrTargetHTable(),"UsedTime",ts,this.getUsedTime());		
		MonitorThread.addMonitorObj(mo);*/
		MonitorObject mo=new MonitorObject(cp.getCdrTargetHTable(),"TaskStat",ts,this.getTaskStat());		
		MonitorThread.addMonitorObj(mo);

	}

}

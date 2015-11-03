package CdrLoadHandler;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.lang.reflect.Constructor;
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
import CdrUtils.HBaseUtils;
import CdrUtils.HadoopUtils;
import CdrUtils.StrUtils;

public class BulkLoadWorker implements Runnable {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

	File F = null;

	private long fileSize = 0L;
	private long rowsCount = 0L;
	private long reduceRowCount=0L;
	private long skippedRowsCount = 0L;
	private String status = "RUNNING";
	private CdrParser cp = null;
	private long starttime = 0L;
	private long endtime = 0L;
	private long endtime2 = 0L;
	private int loadSpeed = 0;
	private long indexCount = 0L;

	// public BulkLoadWorker(File f)
	// {
	// this.F=f;

	// }
	public BulkLoadWorker(File f, CdrParser p) {
		this.F = f;
		this.cp = p;
	}

	public long getFileSize() {
		return F.length();
	}

	public long getRowsCount() {
		return rowsCount;
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub

		this.starttime = System.currentTimeMillis();
		// parse file->file date,htable name,rowkey...
		CLogger.log4j("INFO", "Worker Started, Processing: " + F.getAbsolutePath());

		RetryQueue retryQueue = new RetryQueue();
		boolean isAdd2RetryQueue=true;

		try {
			// after a long wait, the file may be deleted or move!
			if (!F.exists()) {
				// 文件被删除或者被移动，直接退出
				// JobQueue.removeFromJobQueue(F.getAbsolutePath());
				CLogger.log4j("ERROR", "Worker Terminated, File Not Found: " + F.getAbsolutePath());
				this.status = "FILE_NOT_FOUND";
				retryQueue.removeFromRetry(F);
				return;
			}
			//创建hbase表
			HBaseUtils.createTableIfNotExist(cp);
			//上传文件到指定目录
			HadoopUtils.uploadFile2HDFS(F.getAbsolutePath(), cp.getHDFSUploadDir() + F.separator + F.getName());
			HadoopUtils.deleteHDFSFile(cp.getBulkLoadDir());
			
			StringBuffer cmd = new StringBuffer("hadoop jar bulkload.jar ");
			cmd.append(cp.getCdrTargetHTable() + " " +cp.geColumnName()+" "+ cp.getCdrPreBuildRegionNum());
			cmd.append(" " +cp.getPrimaryKeyIndex()+" "+cp.getCdrDate());
			cmd.append(" " + cp.getHDFSUploadDir() + " " + cp.getBulkLoadDir()+" "+cp.getDelim());
			
			cmd.append(" 2>&1|tee ./log/"+F.getName()+"#bulkload-"+StrUtils.long2datestr(System.currentTimeMillis())+".log");
			//System.out.println(cmd);

			// Runtime.getRuntime().exec(cmd.toString());

			// do something here
			/**
			 * hadoop jar bulkloader.jar target_table,pre-bulid_regionNum,msisdnIdx,ts,upload_dir,bulkload_dir
			 */
			
			Process proc=Runtime.getRuntime().exec(new String[]{"/bin/sh","-c",cmd.toString()});
			//当任务提交到exec后,发生异常将不会将任务加入到重试队列,避免数据重复加载
			isAdd2RetryQueue=false;
			InputStreamReader r=new InputStreamReader(proc.getInputStream());
			LineNumberReader lr=new LineNumberReader(r);
			String line;
			while((line=lr.readLine())!=null)
			{
				System.out.println(line);
				if(line.indexOf("Map input records")!=-1)
					this.rowsCount=getMRCounter(line,"Map input records");
				if(line.indexOf("Reduce output records")!=-1)
				{
					this.reduceRowCount=getMRCounter(line,"Reduce output records");
				
					this.skippedRowsCount=this.rowsCount-this.reduceRowCount;
				}
					
			}
			proc.waitFor();
				
			
			System.out.println("==>run finished!");
			this.endtime = System.currentTimeMillis();
			this.status = "SUCCESSED";

			// maybe this job comes from retry queue, so delete it
			retryQueue.removeFromRetry(F);
			CLogger.logProcessedFiles(getTaskStat());

		} catch (Exception e) {
			// TODO Auto-generated catch block
			this.status = "FAILED";
			e.printStackTrace();
			CLogger.log4j("ERROR", "BulkLoad Worker thread Exception, " + e.toString() + "," + F.getAbsolutePath());
			CLogger.logStackTrace(e);
			// 可能是网络、hbase集群等问题导致到异常，将文件加入到重试列表
			// push to retry queue,
			if(isAdd2RetryQueue)
				retryQueue.push2Retry(F);
			

		} finally {
			// remove from job queue
			JobQueue.removeFromJobQueue(F.getAbsolutePath());

		}

		// add2Monitor();

		// CLogger.logProcessedFiles(getTaskStat());

	}
	public long getMRCounter(String row,String cname)
	{
		int pos=row.indexOf(cname);
		//System.out.println("===================>"+row.substring(pos+1+cname.length()),row.length()));
		return Long.parseLong(row.substring(pos+1+cname.length(),row.length()));
	}
	public long getUsedTime() {
		return this.endtime - this.starttime;
	}

	public int getLoadSpeed() {
		return (int) (this.reduceRowCount * 1000 / this.getUsedTime());
	}

	public long getSkippedRowsCount() {
		return this.skippedRowsCount;
	}

	public long getIndexRowCount() {
		return this.indexCount;
	}

	public long getIndexTotalSize() {
		return this.indexCount * 40;
	}

	private String getTaskStat() {
		String s;
		if (cp != null)
			s = "File Name:" + cp.getCdrName() + ",Firm:" + cp.getCdrFirm() + ",Target Table:" + cp.getCdrTargetHTable()
					+ ",";
		else
			s = "File Name:" + this.F.getName() + "\n";

		s += "Status:" + this.status + ",File Size:" + this.getFileSize() + " bytes,TotalRowsCount(data+index):"
				+ (this.getRowsCount() + this.getIndexRowCount()) + ",SkippedRowsCount:" + this.getSkippedRowsCount()
				+ ",Used Time:" + this.getUsedTime() + " ms, Load Speed: " + this.getLoadSpeed() + " rows/s"
				+ ",IndexRowCount:" + this.getIndexRowCount() + ",IndexTotalSize:" + this.getIndexTotalSize()
				+ " bytes";
		return s;
	}

	//
	private void add2Monitor() {
		long ts = System.currentTimeMillis();
		/*
		 * MonitorObject mo=new
		 * MonitorObject(cp.getCdrTargetHTable(),"RowsCount",ts,this.
		 * getRowsCount()); MonitorThread.addMonitorObj(mo); mo=new
		 * MonitorObject(cp.getCdrIndexHTable(),"IndexRowsCount",ts,this.
		 * getIndexRowCount()); MonitorThread.addMonitorObj(mo); mo=new
		 * MonitorObject(cp.getCdrTargetHTable(),"SkippedRowsCount",ts,this.
		 * getSkippedRowsCount()); MonitorThread.addMonitorObj(mo); mo=new
		 * MonitorObject(cp.getCdrTargetHTable(),"FileSize",ts,this.getFileSize(
		 * )); MonitorThread.addMonitorObj(mo); mo=new
		 * MonitorObject(cp.getCdrTargetHTable(),"UsedTime",ts,this.getUsedTime(
		 * )); MonitorThread.addMonitorObj(mo);
		 */
		MonitorObject mo = new MonitorObject(cp.getCdrTargetHTable(), "TaskStat", ts, this.getTaskStat());
		MonitorThread.addMonitorObj(mo);

	}

}

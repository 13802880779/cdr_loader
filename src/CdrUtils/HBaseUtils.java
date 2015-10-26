package CdrUtils;



import java.io.IOException;

import org.apache.commons.lang.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.regionserver.ConstantSizeRegionSplitPolicy;
import org.apache.hadoop.hbase.util.Bytes;
import org.jboss.netty.util.internal.StringUtil;

import CdrConfiguration.CConf;
import CdrLogger.CLogger;
import CdrParser.CdrParser;

public class HBaseUtils {
	
	public static void main(String[] args) throws IOException {
	   // final String QUORUM = "FBI001,FBI002,FBI003";
	   // final String CLIENTPORT = "2181";
	//	updateTableMetaWhenPropertiesFileChanged();
		updateMetaTableWhenPropertiesFileChanged();
        
    }

	private static Configuration conf;
	private static HConnection conn;
	
	static{
		try {
			conf= HBaseConfiguration.create();
			conn=HConnectionManager.createConnection(conf);
			//创建元数据表
			createTableIfNotExist1(CConf.getAppMetaTableName());
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			CLogger.log4j("ERROR","HBaseUtils,Create connection exception:"+e.toString());
			CLogger.logStackTrace(e);
		}
	}
	
	public HBaseUtils()
	{
		//String s[]=StringUtils.split("");
	}
	
	public static HConnection getConnection()
	{
		if(conn==null){
			try {
				if(conf==null)
					conf= HBaseConfiguration.create();
				
				conn=HConnectionManager.createConnection(conf);
				//conn.
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				CLogger.log4j("ERROR","HBaseUtils,Create connection exception:"+e.toString());
				CLogger.logStackTrace(e);
				e.printStackTrace();
				return null;
			}
		}
		return conn;
	}
	public synchronized static void createTableIfNotExist1(String tn) throws MasterNotRunningException, ZooKeeperConnectionException, IOException
	{
		if(conf==null)
			conf= HBaseConfiguration.create();
		HBaseAdmin ha=new HBaseAdmin(conf);
		if(!ha.tableExists(tn))
		{
			HTableDescriptor tableDesc = new  HTableDescriptor(tn);
	    	HColumnDescriptor hd=new HColumnDescriptor("C");
	    	hd.setBloomFilterType(BloomType.ROW);//default ROW
	    	hd.setCompactionCompressionType(Compression.Algorithm.SNAPPY);
	    	hd.setCompressionType(Compression.Algorithm.SNAPPY);
	    	hd.setMaxVersions(1);
	    	hd.setTimeToLive(CConf.getHtableTTL());
	    	tableDesc.addFamily(hd);
	    	ha.createTable(tableDesc);
	    	ha.close();	    		
	    	CLogger.log4j("INFO","HBaseUtils,Created new table:"+tn);	    	

		}	

	}
	public synchronized static void createTableIfNotExist(CdrParser p) throws MasterNotRunningException, ZooKeeperConnectionException, IOException
	{
		if(conf==null)
			conf= HBaseConfiguration.create();
		HBaseAdmin ha=new HBaseAdmin(conf);
		boolean needUpdateMeta=false;
		if(!ha.tableExists(p.getCdrTargetHTable()))
		{
			HTableDescriptor tableDesc =  new   HTableDescriptor(p.getCdrTargetHTable());
	    	HColumnDescriptor hd=new HColumnDescriptor("C");
	    	hd.setBloomFilterType(BloomType.ROW);//default ROW
	    	hd.setCompactionCompressionType(Compression.Algorithm.SNAPPY);
	    	hd.setCompressionType(Compression.Algorithm.SNAPPY);
	    	hd.setMaxVersions(1);
	    	hd.setTimeToLive(CConf.getHtableTTL());
	    	tableDesc.addFamily(hd);
	    	ha.createTable(tableDesc,getIntPartionSplitKeys(p.getCdrPreBuildRegionNum()));
	    		
	    	CLogger.log4j("INFO","HBaseUtils,Created new table:"+p.getCdrTargetHTable());
	    	
	    	needUpdateMeta=true;
	    	//addTableMeta(p);
		}	
		
		//判断是否需要创建二级索引表
		if(p.isSecondaryIndex()&&!ha.tableExists(p.getCdrIndexHTable()))
		{
			HTableDescriptor tableDesc =  new   HTableDescriptor(p.getCdrIndexHTable());
	    	HColumnDescriptor hd=new HColumnDescriptor("C");
	    	hd.setBloomFilterType(BloomType.ROW);//default ROW
	    	hd.setCompactionCompressionType(Compression.Algorithm.SNAPPY);
	    	hd.setCompressionType(Compression.Algorithm.SNAPPY);
	    	hd.setMaxVersions(1);
	    	hd.setTimeToLive(CConf.getHtableTTL());
	    	tableDesc.addFamily(hd);
	    	ha.createTable(tableDesc,getIntPartionSplitKeys(p.getSecondaryIndexPreBuildRNum()));	    		
	    	CLogger.log4j("INFO","HBaseUtils,Created new table:"+p.getCdrIndexHTable()+", prebuild region num: "+p.getSecondaryIndexPreBuildRNum());
	    	needUpdateMeta=true;
		}
		ha.close();	   
		ha=null;
		
		if(needUpdateMeta)
			updateTableMeta(p);
	}
	
	public synchronized static void updateTableMeta(CdrParser p) throws IOException
	{
		
		HTableInterface htable=getConnection().getTable(CConf.getAppMetaTableName());
		try{
		byte[] rk=Bytes.toBytes(p.getCdrTargetHTable());
		Put p1=new Put(rk);
		p1.add(Bytes.toBytes("C"), Bytes.toBytes("RegionsNum"), Bytes.toBytes(p.getCdrPreBuildRegionNum()));
		if(p.isSecondaryIndex())
		{
			p1.add(Bytes.toBytes("C"), Bytes.toBytes("IdxRegionsNum"), Bytes.toBytes(p.getSecondaryIndexPreBuildRNum()));
			p1.add(Bytes.toBytes("C"), Bytes.toBytes("IndexConf"), Bytes.toBytes(p.getIndexConfiguration()));

		}
		htable.put(p1);
		htable.flushCommits();
		CLogger.log4j("INFO","HBaseUtils,table meta updated:"+p.getCdrTargetHTable());
		}finally{
		htable.close();
		}
		
	}
	
	
	
	public synchronized static void updateMetaTableWhenPropertiesFileChanged() throws IOException
	{
		HTableInterface htable=getConnection().getTable(CConf.getAppMetaTableName());
		try{
		String firms[]=CConf.getCdrFirms();
		for(String firm:firms)
		{
			String prefixs[]=CConf.getCdrPrefix(firm);
			for(String prefix:prefixs)
			{
				//String s=prefix.replaceAll("-cdr", "").replaceAll("-tdr", "");
				int pos1=prefix.indexOf("-cdr")==-1?prefix.indexOf("-tdr"):prefix.indexOf("-cdr");
				String cPrefix=prefix.substring(0, pos1);
				//System.out.println(firm+","+cPrefix);
				String idxConf=CConf.getCdrIndexConfiguration(firm, cPrefix);
				int RegionNum=CConf.getCdrRegionNum(firm, cPrefix);
				int IdxRegionNum=CConf.getCdrIndexRegionNum(firm, cPrefix);
				
				
				
				
				String targettable=prefix.replaceAll("-", "_");
				targettable=targettable.toUpperCase();
				//int rNum=
				//System.out.println(targettable+","+idxConf);
				byte[] rk=Bytes.toBytes(targettable.toUpperCase());
				Put p1=new Put(rk);
				p1.add(Bytes.toBytes("C"), Bytes.toBytes("RegionsNum"), Bytes.toBytes(RegionNum));
				p1.add(Bytes.toBytes("C"), Bytes.toBytes("IdxRegionsNum"), Bytes.toBytes(IdxRegionNum));
				if(idxConf!=null)
					p1.add(Bytes.toBytes("C"), Bytes.toBytes("IndexConf"), Bytes.toBytes(idxConf));
				htable.put(p1);				
				
			}
			
		}
		htable.flushCommits();
		System.out.println("all  meta data updated!");
		CLogger.log4j("INFO","HBaseUtils,Update all metadata completed!");
		}finally{		
		htable.close();		
		}
	}
	
	public static byte[] getTableMeta(String tn,String metaName) throws IOException
	{
		//System.out.println(CConf.getCdrMetaTable());
		HTableInterface htable=getConnection().getTable(CConf.getAppMetaTableName());
		//System.out.println(metaName);
		byte[] rk=Bytes.toBytes(tn);
		Get g=new Get(rk);
		g.addColumn(Bytes.toBytes("C"), Bytes.toBytes(metaName));
		Result result = htable.get(g);  
		byte[] val = result.getValue(Bytes.toBytes("C"), Bytes.toBytes(metaName));
	//	System.out.println("val==>"+Bytes.toInt(val));
		
		htable.close();
		return val;
	}
	
	public synchronized static void createTableIfNotExist(String tn) throws MasterNotRunningException, ZooKeeperConnectionException, IOException
	{
		if(conf==null)
			conf= HBaseConfiguration.create();
		HBaseAdmin ha=new HBaseAdmin(conf);
		if(!ha.tableExists(tn))
		{
			HTableDescriptor tableDesc =  new   HTableDescriptor(tn);
			tableDesc.setValue(HTableDescriptor.SPLIT_POLICY, ConstantSizeRegionSplitPolicy.class.getName());
	    	HColumnDescriptor hd=new HColumnDescriptor("C");
	    	hd.setBloomFilterType(BloomType.ROW);//default ROW
	    	hd.setCompactionCompressionType(Compression.Algorithm.SNAPPY);
	    	hd.setCompressionType(Compression.Algorithm.SNAPPY);
	    	hd.setMaxVersions(1);
	    	tableDesc.addFamily(hd);
	    	ha.createTable(tableDesc);
	    	ha.close();
	    	
	    	CLogger.log4j("INFO","HBaseUtils,Created new table:"+tn);
		}

	}
	

	public static byte[][] getIntPartionSplitKeys(int numPartions)
	{
		byte[][] SplitKeys=new byte[numPartions-1][];
		for(int i=1; i<numPartions; i++)
		{
			SplitKeys[i-1]=Bytes.toBytes(i);
		}		
		return SplitKeys;
		
	}
}

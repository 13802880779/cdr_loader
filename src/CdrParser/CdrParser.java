package CdrParser;

import java.io.File;
import java.util.ArrayList;

import org.apache.hadoop.hbase.client.Durability;

import CdrConfiguration.CConf;
import CdrExceptions.FileParseException;
import CdrExceptions.FirmNotFoundException;

public abstract class CdrParser {
	File F=null;
	String cName="cdr";
	String cPrefix="cdr";
	String cFirm="unknown";
	String cDate="1970-01-01";
	String cTargetTable="cdrtable";
	String cIndexTable="indextable";
	int cPreBuildRegionNum=-1;
	int cPreBuildIndexTableRegionNum=-1;
	String cDelim=",";
	int cMsisdnIdx=-1;
	int cTimeStampIdx=-1;
	int cColumnCount=-1;
	int cXdrIdIndex=-1;
	boolean isSecondaryIndex=false;
	String cSecIdxConfiguration="";
	CdrIndexParser csip;
	Durability cDurablity=Durability.SYNC_WAL;
	String cLoadType="batch_put";
	String cColumnName="D";
		
	public CdrParser(File f)
	{
		this.F=f;
		//parse();
	}
	/**
	 * parse cdr info from cdr filename;
	 * example:aiu-mm-cdr-201504291355-00001_20150429#20150429140402#.dat
	 */
	abstract public int parse();
	abstract public byte[] generatetRowKey(final String[] r);
	abstract public ArrayList<byte[]> generateIndexRowkey(String[] r, int rNum,byte[] rk);
	abstract public void actionAfterLoad(File fsrc);
	abstract public String getHDFSUploadDir();
	abstract public String getBulkLoadDir();
	
	
	//abstract public byte[] getIndexRowkey(byte[] idxColName,byte[] idxContent, byte[] )
	public Durability getHtableDurability(){return cDurablity;};
	public String getCdrType(){return cPrefix;}
	public String getCdrFirm(){return cFirm;}
	public String getCdrDate(){return cDate;}
	public String getCdrTargetHTable(){return cTargetTable;}
	public String getCdrIndexHTable(){return this.cTargetTable+"_INDEX";}
	//public String getCdrIndexHTable(){return cIndexTable;}
	public String getCdrName(){return cName;}
	public int getCdrPreBuildRegionNum(){return cPreBuildRegionNum;}
	public boolean isSecondaryIndex(){ return this.isSecondaryIndex;}
	public int getSecondaryIndexPreBuildRNum(){return this.cPreBuildIndexTableRegionNum;}
	public String getIndexConfiguration(){return CConf.getCdrIndexConfiguration(cFirm, cPrefix);}
	public String getDelim(){return this.cDelim;}
	public String getPrefix(){return this.cPrefix;};
	public String getSecondaryIndexConfiguration(){return this.cSecIdxConfiguration;}
	public String getLoadType(){return this.cLoadType;}
	public int getPrimaryKeyIndex(){return this.cMsisdnIdx;}
	public String geColumnName(){return this.cColumnName;}
	public String toString(){
		return cName+"#"+cFirm+"#"+cPrefix+"#"+cDate+"#"+cTargetTable+"#"+cPreBuildRegionNum;
	}
	
	

	public static String findFirmByFileName (String fname) throws FirmNotFoundException
	{
		String firms[]=CConf.getCdrFirms();
		if(firms==null)
			throw new FirmNotFoundException("'cdr.firms' not set in job.properties!");
		//	return null;
		
		for(String firm:firms)
		{
			String prefixs[]=CConf.getCdrPrefix(firm);
			if(prefixs==null)
				continue;
			for(String prefix:prefixs)
			{
				if(fname.startsWith(prefix))
					return firm;
			}
		}
		
		throw new FirmNotFoundException("Can not determine firm name by "+fname);
	//	return null;

	}
	

}

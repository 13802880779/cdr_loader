package CdrParser;

import java.io.File;
import java.util.ArrayList;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.util.Bytes;

import CdrConfiguration.CConf;
import CdrLogger.CLogger;

public class NokiaCdrParser extends CdrParser {

	public NokiaCdrParser(File f) {
		super(f);
		// TODO Auto-generated constructor stub
	}

	@Override
	public int parse() {
		// TODO Auto-generated method stub
		try{
		this.cName=F.getName();
		this.cFirm="nokia";
		this.cPrefix=this.cName.substring(0, this.cName.indexOf("_uploading"));
		
		int pos=this.cName.lastIndexOf("-");
		this.cDate=this.cName.substring(pos-12, pos-4);
		this.cTargetTable=CConf.getCdrHTableName(cFirm,cPrefix).replaceAll("%DATE", cDate);
		this.cPreBuildRegionNum=CConf.getCdrRegionNum(cFirm, cPrefix);
		this.cDelim=CConf.getCdrDelim(cFirm, cPrefix);
		this.cMsisdnIdx=CConf.getCdrPrimaryKeyIdx(cFirm, cPrefix);
		//this.cTimeStampIdx=CCONF.getTimeStampIdx(cFirm, cPrefix);
		this.cColumnCount=CConf.getCdrColumnCount(cFirm, cPrefix);
		}catch(Exception e)
		{
			CLogger.log4j("ERROR","NokiaCdrParser.java, parse error,"+e.toString());
			CLogger.logStackTrace(e);
			return -1;
		}
		return 1;
	
	}

	@Override
	public byte[] generatetRowKey(String[] r) {
		// TODO Auto-generated method stub
		//String r[]=StrUtils.split(row, this.cDelim);
		
		if(this.cColumnCount!=-1 && (r.length!=this.cColumnCount))
			return null;
		
		
		String revMsisdn=StringUtils.leftPad((new StringBuffer(r[this.cMsisdnIdx])).reverse().toString(),11,"0");
		
		int salt=Math.abs(revMsisdn.hashCode())%this.cPreBuildRegionNum;
		
		long ts=Long.parseLong(r[18]);
		long type=Long.parseLong(r[19]);
		//rowkey.re
	//	System.out.println("salt==>"+salt+" revmsisdn==>"+revMsisdn+" ts==>"+StrUtils.long2datestr(ts));
		
		
		return Bytes.add(Bytes.add(Bytes.toBytes(salt), 
                					Bytes.toBytes(revMsisdn),
                					Bytes.toBytes(ts)), 
                        Bytes.toBytes(type));
		
	}

	@Override
	public ArrayList<byte[]> generateIndexRowkey(String[] cols, int rNum, byte[] rk) {
		// TODO Auto-generated method stub
		return null;
	}



	@Override
	public void actionAfterLoad(File fsrc) {
		// TODO Auto-generated method stub
		
	}

}

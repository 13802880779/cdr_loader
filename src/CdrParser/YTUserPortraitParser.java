package CdrParser;

import java.io.File;
import java.util.ArrayList;

import CdrConfiguration.CConf;
import CdrLogger.CLogger;
import CdrUtils.StrUtils;

public class YTUserPortraitParser extends CdrParser {

	public YTUserPortraitParser(File f) {
		super(f);
		// TODO Auto-generated constructor stub
	}


	/**
	 * 	f_hlr2_lb_month-201502
		f_noas_t_u_history-201502
		f_hlr2_facebook_month-201502
		f_ps_a_u_month
		f_ps_music_month
		f_ps_s_u_month
		f_ps_shopping_month
		f_ps_book_month
		f_ps_k_u_month
		f_ps_phone_month
		f_cs_h_u_month
		f_ps_h_u_month
		f_ps_c_h_u_month
		f_ps_a_h_u_month
	 */
	private long cDateID=0L;
	public int parse() {
		// TODO Auto-generated method stub
		try{
		this.cName=F.getName();
		this.cFirm="yt";
		
		int pos1=this.cName.indexOf("-");
		this.cPrefix=this.cName.substring(0, pos1);
		
		int pos=this.cPrefix.length()+1;		
		this.cDate=this.cName.substring(pos, pos+6);
		this.cDateID=StrUtils.datestr2long3(this.cDate);
		this.cTargetTable=CConf.getCdrHTableName(cFirm,cPrefix);
		this.cPreBuildRegionNum=CConf.getCdrRegionNum(cFirm, cPrefix);
		this.cDelim=CConf.getCdrDelim(cFirm, cPrefix);
		this.cMsisdnIdx=CConf.getCdrPrimaryKeyIdx(cFirm, cPrefix);
		
		//this.cXdrIdIndex=CConf.getCdrXidIndex(cFirm, cPrefix);
		//this.ts1idx=CConf.getCdrTimeStampIdx(this.cFirm,this.cPrefix, 1);
		//this.ts2idx=CConf.getCdrTimeStampIdx(this.cFirm,this.cPrefix, 2);
		//this.path2egnore=CConf.getPath2EgnoreSuffix(cFirm);
		//this.path2handle=CConf.getPath2HandleSuffix(cFirm);
		this.isSecondaryIndex=CConf.getCdrIndexConfiguration(cFirm, cPrefix)==null?false:true;
		this.cPreBuildIndexTableRegionNum=CConf.getCdrIndexRegionNum(cFirm, cPrefix);
		this.cSecIdxConfiguration=CConf.getCdrIndexConfiguration(cFirm, cPrefix);
		if(this.isSecondaryIndex)
			csip=new CdrIndexParser(this.cSecIdxConfiguration);
		
		//this.cTimeStampIdx=CCONF.getTimeStampIdx(cFirm, cPrefix);
		this.cColumnCount=CConf.getCdrColumnCount(cFirm, cPrefix);
		//this.cDurablity=CConf.getHtableDurability(cFirm, cPrefix);
		this.cLoadType=CConf.getCdrLoadType(cFirm, cPrefix);
		this.cColumnName=CConf.getColumnName(cFirm, cPrefix);
		//System.out.println("cLoadType==>"+this.cLoadType);
		}catch(Exception e)
		{
			CLogger.log4j("ERROR","YTUserPortraitParser.java, parse error,"+e.toString());
			CLogger.logStackTrace(e);
			return -1;
		}
		return 1;
			
	}
	
	public String getHDFSUploadDir()
	{
		return "/tmp/"+this.getPrefix()+"-"+this.getCdrDate();
	}
	public String getBulkLoadDir()
	{
		return "/tmp/"+this.getPrefix()+"-bulkload-"+this.getCdrDate();
	}

	@Override
	public byte[] generatetRowKey(String[] r) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ArrayList<byte[]> generateIndexRowkey(String[] r, int rNum, byte[] rk) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void actionAfterLoad(File fsrc) {
		// TODO Auto-generated method stub

	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}

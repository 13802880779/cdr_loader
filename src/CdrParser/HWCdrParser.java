package CdrParser;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;

import org.apache.commons.lang.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

import CdrConfiguration.CConf;
import CdrLogger.CLogger;
import CdrUtils.StrUtils;

public class HWCdrParser extends CdrParser{
	
	private int ts1idx=1;
	private int ts2idx=2;
	//private String path2handle="";
	//private String path2egnore="";
	public static void main(String args[])
	{
		//String firm=
		/*try {
			String firm=CdrParser.findFirmByFileName("aiu-mm-cdr-201504291355-00001_20150429#20150429140402#.dat");

			Class<?> c=Class.forName(CCONF.getCdrParserClassName(firm));
			Constructor<?> c1=c.getDeclaredConstructor(File.class);
			c1.setAccessible(true);
			CdrParser cp=(CdrParser)c1.newInstance(new File("data_src/cs/aiu-mm-cdr-201504291355-00001_20150429#20150429140402#.dat"));
			System.out.println(cp.toString());
			

			
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NoSuchMethodException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
		//System.out.println(System.currentTimeMillis());
		File f=new File("data_src/cs/aiu-ho-cdr-201504291355-00001_20150429#20150429140402#.dat");
		HWCdrParser p=new HWCdrParser(f);
		//System.out.println(p.toString());
		try {
			BufferedReader br=new BufferedReader(new FileReader(f));
			String row;
			while((row=br.readLine())!=null)
				{
					try{
						p.generatetRowKey(StrUtils.split(row,p.getDelim()));
					}catch(Exception ex)
					{
						System.out.println("exception e test");
						//continue;
					}
				}
			
			br.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
	}

	public HWCdrParser(File f) {
		super(f);
		// TODO Auto-generated constructor stub
	}

	@Override
	/**
	 * aiu-mm-cdr-201504291355-00001_20150429#20150429140402#.dat
	 */
	public int parse() {
		// TODO Auto-generated method stub
		//the file may be deleted!
		try{
		this.cName=F.getName();
		this.cFirm="hw";
		
		int pos1=this.cName.indexOf("-cdr")==-1?this.cName.indexOf("-tdr"):this.cName.indexOf("-cdr");
		this.cPrefix=this.cName.substring(0, pos1);
		
		int pos=this.cPrefix.length()+"-cdr-".length();		
		this.cDate=this.cName.substring(pos, pos+8);
		this.cTargetTable=CConf.getCdrHTableName(cFirm,cPrefix);
		this.cPreBuildRegionNum=CConf.getCdrRegionNum(cFirm, cPrefix);
		this.cDelim=CConf.getCdrDelim(cFirm, cPrefix);
		this.cMsisdnIdx=CConf.getCdrPrimaryKeyIdx(cFirm, cPrefix);
		this.cXdrIdIndex=CConf.getCdrXidIndex(cFirm, cPrefix);
		this.ts1idx=CConf.getCdrTimeStampIdx(this.cFirm,this.cPrefix, 1);
		this.ts2idx=CConf.getCdrTimeStampIdx(this.cFirm,this.cPrefix, 2);
		//this.path2egnore=CConf.getPath2EgnoreSuffix(cFirm);
		//this.path2handle=CConf.getPath2HandleSuffix(cFirm);
		this.isSecondaryIndex=CConf.getCdrIndexConfiguration(cFirm, cPrefix)==null?false:true;
		this.cPreBuildIndexTableRegionNum=CConf.getCdrIndexRegionNum(cFirm, cPrefix);
		this.cSecIdxConfiguration=CConf.getCdrIndexConfiguration(cFirm, cPrefix);
		if(this.isSecondaryIndex)
			csip=new CdrIndexParser(this.cSecIdxConfiguration);
		
		//this.cTimeStampIdx=CCONF.getTimeStampIdx(cFirm, cPrefix);
		this.cColumnCount=CConf.getCdrColumnCount(cFirm, cPrefix);
		this.cDurablity=CConf.getHtableDurability(cFirm, cPrefix);
		
		}catch(Exception e)
		{
			CLogger.log4j("ERROR","HWCdrParser.java, parse error,"+e.toString());
			CLogger.logStackTrace(e);
			return -1;
		}
		return 1;
			
		
	

	}

	@Override
	/**
	 * 返回每行数据到rowkey,salt(int)+revmsisdn(string,11bytes)+timestamp(long)
	 */
	public byte[] generatetRowKey(String[] r){
		// TODO Auto-generated method stub
		//[4][8][8][4]
		//String r[]=rowItems;
		
		if((this.cColumnCount!=-1 && (r.length<this.cColumnCount))||this.cXdrIdIndex==-1)
			return null;
		
		try{
		String revMsisdn;
		long msisdn;
		
		if(StrUtils.isBlankString(r[this.cMsisdnIdx]))//如果是空号码，那么生成一个随机的18位到数字
		{
			msisdn=(long)(Math.random()*1000000000000000000L);//生成一个随机到18位数字
			revMsisdn=StringUtils.rightPad(String.valueOf(msisdn), 18,"0");
			//补全到16位到长整数
			msisdn=Long.parseLong(revMsisdn);
		}
		else{
		//对小于11位的号码，如固话号码，左补0到11位（正常到msisdn位长)
		 revMsisdn=StringUtils.leftPad((new StringBuffer(r[this.cMsisdnIdx])).reverse().toString(),11,"0");
		 //注意，有可能含有非数字字符,正常返回11位到long数字，当以下异常时，将取字符串hashcode，并返回固定位长的long
		 //1）revMsisdn可能包含字符,2)revMsisdn长度可能超过11位
		 //msisdn最长应该在11-15位间，返回16位右补长字段即可
		 msisdn=getAbsoluteLong(revMsisdn,16);
		}
		
		
		int salt=Math.abs(revMsisdn.hashCode())%this.cPreBuildRegionNum;
		
		long ts=Long.parseLong(r[this.ts1idx])*1000+Long.parseLong(r[this.ts2idx]);
	
		//随机生成位数，所以重新扫描同一个文件会生成不同到rowkey
		//int rnum=(int)(Math.random()*Integer.MAX_VALUE);
		
		
		return Bytes.add(Bytes.add(Bytes.toBytes(salt), 
                        Bytes.toBytes(msisdn),
                        Bytes.toBytes(ts)),Bytes.toBytes(r[this.cXdrIdIndex].hashCode()));
	
		}catch(Exception e){
			//e.printStackTrace();
			CLogger.log4j("ERROR","HWCdrParser, generatetRowKey Exception, "+e.toString());
			//CLogger.logStackTrace(e);
			return null;
		}
		
	}
	

	/**
	 * 【索引salt，4位】+【索引值，12位】+【时间戳，8位】+【rowkey，24位】
	 */
	
	public ArrayList<byte[]> generateIndexRowkey(String[] r, int rNum, byte[] rk)
	{
	
		ArrayList<byte[]> rkAL=new ArrayList();
		
		try{
		long ts=Long.parseLong(r[this.ts1idx])*1000+Long.parseLong(r[this.ts2idx]);
		
		//如果在index configuration parser中发生异常，则不会生成任何到index key
		for(IndexObj obj:csip.getParseIdxObj())
		{
			
			byte[] rk1=new byte[12];//保存索引值
			
			//StringBuffer info=new StringBuffer("");	
			
			String content[]=new String[obj.ctnNum];
			StringBuffer sb=new StringBuffer();//保存combine类型索引的合并字符串
			for(int i=0; i<obj.colIdx.length;i++)
			{
				if(r[obj.colIdx[i]].length()>obj.ctnLen[i])
				{
					content[i]=StringUtils.rightPad(String.valueOf(Math.abs(r[obj.colIdx[i]].hashCode())), obj.ctnLen[i],"0");
							//r[obj.colIdx[i]].substring(0, obj.ctnLen[i]);
					//CLogger.log4j("ERROR","HWCdrParser, generateIndexRowkey, index key too long,:"+r[obj.colIdx[i]]+",prefer len:"+obj.ctnLen[i]);
				}
				else					
					content[i]=StringUtils.leftPad(r[obj.colIdx[i]], obj.ctnLen[i], "0");					
				sb.append(content[i]);
			}
			
			int salt=Math.abs(sb.toString().hashCode())%rNum;

			//info.append("salt=>"+salt);
			
			int offset=0;
			offset=Bytes.putBytes(rk1, offset, obj.head.getBytes(), 0, obj.head.length());
			
			//info.append(" index_name=>"+obj.head);
			//info.append(" index_value=>");
			for(int i=0;i<content.length; i++)
			{
				//System.out.println("==>"+content[i].hashCode());
				switch(obj.dataType[i]){
				case "INT":
					offset=Bytes.putInt(rk1, offset, Integer.parseInt(content[i]));
				//	info.append(" int:"+Integer.parseInt(content[i]));
					break;
				case "LONG":
					offset=Bytes.putLong(rk1, offset, getAbsoluteLong(content[i],obj.ctnLen[i]));
				//	info.append(" long:"+Long.parseLong(content[i]));
					break;
				case "SHORT":
					offset=Bytes.putShort(rk1, offset, Short.parseShort(content[i]));
					//info.append(" short:"+Short.parseShort(content[i]));
					break;
				case "STR":
				default:
					offset=Bytes.putBytes(rk1, offset, content[i].getBytes(), 0, obj.ctnLen[i]);					
					//info.append(" str:"+content[i]);
				}
			}

			//info.append(" ts==>"+ts);
			//System.out.println(info.toString());
			byte[] rk_tmp=new byte[16];
			rk_tmp=Bytes.add(Bytes.copy(rk, 0, 12),Bytes.copy(rk, 20, 4));
			rkAL.add(Bytes.add(Bytes.add(Bytes.toBytes(salt),rk1,Bytes.toBytes(ts)),rk_tmp));
			
			
		}
		
		}catch(Exception e){
			CLogger.log4j("ERROR","HWCdrParser, generateIndexRowkey Exception, "+e.toString()+"\n");
			CLogger.logStackTrace(e);
		}
		finally{
			return rkAL;
		}
	}
	
	public long getAbsoluteLong(String str,int fixLen)
	{
		try{
			return Long.parseLong(str);
		}catch(NumberFormatException e)
		{
			//返回18位的long数字,通过补全str到hashcode
			return Long.parseLong(StringUtils.rightPad(String.valueOf(Math.abs(str.hashCode())), fixLen,"0"));
		}
	}

	@Override
	public void actionAfterLoad(File fsrc) {
		// TODO Auto-generated method stub
		if(CConf.getActionAfterLoad("hw")==null||CConf.getMoveToPathPrefix("hw")==null)
			return;
		//by default, move to anohter folder
		String MoveToPath=fsrc.getParent().replaceFirst(CConf.getFromPathPrefix("hw"), CConf.getMoveToPathPrefix("hw"));
		try{
		File fdest=new File(MoveToPath);
		if(!fdest.exists())
			fdest.mkdirs();
		if(fsrc.exists())
			fsrc.renameTo(fdest);
		}catch(Exception e)
		{
			CLogger.log4j("ERROR", "Move file to folder failed! Dest folder:"+MoveToPath+", Src file:"+F.getAbsolutePath());
			CLogger.logStackTrace(e);
		}
		
	}
	

}

package CdrParser;

import java.util.ArrayList;

import org.apache.commons.lang.*;
import org.apache.hadoop.hbase.util.Bytes;

import CdrConfiguration.CConf;
import CdrLogger.CLogger;
import CdrUtils.StrUtils;


/**
 * 
 * @author
 * @comment:读入二级索引到配置信息，并分解成独立到index object，object信息包括索引名称，索引由那些字段组成，这些字段
 * 与原cdr的映射关系，字段存储到数据类型等
 *
 */
public class CdrIndexParser {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String test="1404885842,1404884823,837,0,0,3,0,3,10884,11511,0,0,460029949092324,34AB41D0,15976917822,359738045024033,460,00,2775,8D2A,00,2775,7CC3,,,,12,,,,,,,8,,,,0,,,,08cf07305a162bed,,,0,2775,7CC3,0,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,08cf03305a1487a8,";
		String ss[]=StrUtils.split(test, ",");
		CdrIndexParser sp=new CdrIndexParser(CConf.getCdrIndexConfiguration("hw", "aiu-ho"));
		//sp.getSecondaryIndexRowkey(ss,100,0L,new byte[24]);

		//System.out.println(b1.hashCode());
		//System.out.println(b1.hashCode());
		//System.out.println(b2.hashCode());
		
	}
	ArrayList<IndexObj> al=new ArrayList<IndexObj>(); //存储所有索引对象
	String tableName="";
	String IndexTableName="";
	boolean isok=false;
	
	public CdrIndexParser(String tableName,String confstr)
	{
		this.tableName=tableName;
		this.IndexTableName=this.tableName+"_INDEX";
		parse(confstr);
	}
	
	public CdrIndexParser(String confstr)
	{
		parse(confstr);
	}
	
	public ArrayList<IndexObj> getParseIdxObj()
	{
		return al;
	}
	public boolean isParserOK()
	{
		return this.isok;
	}
	
	public IndexObj getIdxObjByName(String idxName)
	{
		for(IndexObj obj:al)
		{
			if(obj.head.equals(idxName))
				return obj;
		}
		return null;
	}
	
	public void parse(String conf)
	{
		if(conf==null)
		{
			//System.out.println("index configuration null!");
			CLogger.log4j("ERROR", "Table index configuration parser exception, configuration is null: "+this.tableName);
			this.isok=false;
			
			return;
		}
		try{
		String s[]=conf.split("#");
		for(String ss:s)
		{
			
			String s1[]=ss.split(":");
			
			if(s1.length!=4)
				continue;
			
			IndexObj obj=new IndexObj(s1[0],
					StrArr2IntArr(s1[1].split(",")),
					StrArr2IntArr(s1[2].split(",")),
					s1[3].split(","));
			al.add(obj);
			
		}
		
		this.isok=true;
		}catch(Exception e)
		{
			CLogger.log4j("ERROR","CdrIndexParser Exception, "+e.toString());
			CLogger.logStackTrace(e);
			this.isok=false;
		}
	}
	

	
	//public 
	
	public int[] StrArr2IntArr(String[] arr)throws NumberFormatException
	{
		int num[]=new int[arr.length];
		for(int i=0; i<arr.length;i++)
			num[i]=Integer.parseInt(arr[i]);
		return num;
	}
	

}

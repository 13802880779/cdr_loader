package CdrParser;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.util.Bytes;

import CdrLogger.CLogger;
import CdrUtils.StrUtils;

public class RowkeyTester {

	public static void main(String[] args) throws FileNotFoundException {
		// TODO Auto-generated method stub

		
		long st=System.currentTimeMillis();//4158,6666,8544,5239,5622
		
		try
		{
			BufferedReader br=new BufferedReader(new FileReader(new File("/data/glassfish/bak/CS_AIU_HO/CITY_86020/aiu-ho-cdr-201510261505-00001_20151026#20151026151502#.dat")));
			String line;
			int rNum=700;
			HashMap<Integer,Integer> hm=new HashMap<Integer,Integer>();
			for(int i=0;i<rNum;i++)
				hm.put(i, 0);
			int cc=0;
			
			while((line=br.readLine())!=null)
			{
				//String r[]=lineParse(line);
				//String r[]=StringUtils.splitPreserveAllTokens(line, ",");//UsedTime:11981,6902,7153,6428
				String r[]=split(line,",");//
				//if(r.length!=)
				//String r[]=StrUtils.split(line, ",");
				//int salt=generatetRowKeySalt(r,14,rNum);
				/*if(salt!=-1)
				{
					int counter=hm.get(salt);
					counter++;
					hm.put(salt, counter);
				}
				else
					cc++;*/
			}
			br.close();
            long et=System.currentTimeMillis();
            System.out.println("UsedTime:"+(et-st));
            
			/*int biggest=0;
			for(int i=0;i<hm.size();i++)
			{
				System.out.println(i+"====>"+hm.get(i));
				if(hm.get(i)>biggest)
					biggest=hm.get(i);
			}
			System.out.println("biggest====>"+biggest);
	
			
			
			Iterator iter = hm2.entrySet().iterator();
			String key="";
			biggest=0;
			while (iter.hasNext()) {
			Map.Entry entry = (Map.Entry) iter.next();
			 key = (String)entry.getKey();
			 int val = (int)entry.getValue();
			 if(val>biggest)
				 biggest=val;
			
			}
			System.out.println("key:"+key+",count:"+biggest);*/
		}catch(Exception e){e.printStackTrace();}

	}
	
	  public static String[] split(String s, String delimiter){
	        if (s == null) {
	            return null;
	        }
	        int delimiterLength;
	        int stringLength = s.length();
	        if (delimiter == null || (delimiterLength = delimiter.length()) == 0){
	            return new String[] {s};
	        }

	        // a two pass solution is used because a one pass solution would
	        // require the possible resizing and copying of memory structures
	        // In the worst case it would have to be resized n times with each
	        // resize having a O(n) copy leading to an O(n^2) algorithm.

	        int count;
	        int start;
	        int end;

	        // Scan s and count the tokens.
	        count = 0;
	        start = 0;
	        while((end = s.indexOf(delimiter, start)) != -1){
	            count++;
	            start = end + delimiterLength;
	        }
	        count++;

	        // allocate an array to return the tokens,
	        // we now know how big it should be
	        String[] result = new String[count];

	        // Scan s again, but this time pick out the tokens
	        count = 0;
	        start = 0;
	        while((end = s.indexOf(delimiter, start)) != -1){
	            result[count] = (s.substring(start, end));
	            count++;
	            start = end + delimiterLength;
	        }
	        end = stringLength;
	        result[count] = s.substring(start, end);

	        return (result);
	    }
	
	public static String[] lineParse(String line) {  
		StringTokenizer st = new StringTokenizer(line, ",", true);  
		  
		        ArrayList<String> fieldList = new ArrayList<String>();  
		        String[] ss = new String[st.countTokens()];  
		        String preField = ",";  
		        for (int i = 0; i < ss.length; i++) {  
		            String field = st.nextToken();  
		            if (StringUtils.equals(preField, ",") && StringUtils.equals(field, ",")) {  
		                fieldList.add("");  
		                preField = field;  
		            } else if (StringUtils.equals(preField, ",") && !StringUtils.equals(field, ",")) {  
		                fieldList.add(field);  
		                preField = field;  
		            } else if (!StringUtils.equals(preField, ",") && StringUtils.equals(field, ",")) {  
		                preField = field;  
		                continue;  
		            }  
		        }  
		        if (StringUtils.equals(preField, ",")) {  
		            fieldList.add("");  
		        }  
		  
		        return fieldList.toArray(new String[fieldList.size()]);  
		}  
	
	  public static void readFileByLine(int bufSize, FileChannel fcin, ByteBuffer rBuffer){ 
	        String enterStr = "\n"; 
	        try{ 
	        byte[] bs = new byte[bufSize]; 

	        int size = 0; 
	        StringBuffer strBuf = new StringBuffer(""); 
	        int rNum=700;
       		HashMap<Integer,Integer> hm=new HashMap<Integer,Integer>();
       		for(int i=0;i<rNum;i++)
       			hm.put(i, 0);
       		int cc=0;
	        //while((size = fcin.read(buffer)) != -1){ 
	        while(fcin.read(rBuffer) != -1){ 
	              int rSize = rBuffer.position(); 
	              rBuffer.rewind(); 
	              rBuffer.get(bs); 
	              rBuffer.clear(); 
	              String tempString = new String(bs, 0, rSize); 
	              //System.out.print(tempString); 
	              //System.out.print("<200>"); 

	              int fromIndex = 0; 
	              int endIndex = 0; 
	              while((endIndex = tempString.indexOf(enterStr, fromIndex)) != -1){ 
	               String line = tempString.substring(fromIndex, endIndex); 
	               line = new String(strBuf.toString() + line); 
	               
	              /* String r[]=StrUtils.split(line, ",");
					int salt=generatetRowKeySalt(r,14,rNum);
					if(salt!=-1)
					{
						int counter=hm.get(salt);
						counter++;
						hm.put(salt, counter);
					}
					else
						cc++;*/
	          

	               strBuf.delete(0, strBuf.length()); 
	               fromIndex = endIndex + 1; 
	              } 
	              if(rSize > tempString.length()){ 
	              strBuf.append(tempString.substring(fromIndex, tempString.length())); 
	              }else{ 
	              strBuf.append(tempString.substring(fromIndex, rSize)); 
	              } 
	        } 
	        } catch (IOException e) { 
	        // TODO Auto-generated catch block 
	        e.printStackTrace(); 
	        } 
	    } 

	public static void  NIOReadTest()
	{
		long st=System.currentTimeMillis();
        String fileName = "/data/glassfish/bak/CS_AIU_MM/CITY_86020/combine.dat";
        int rNum=700;
		HashMap<Integer,Integer> hm=new HashMap<Integer,Integer>();
		for(int i=0;i<rNum;i++)
			hm.put(i, 0);
		int cc=0;
        try {
            List<String> lines = Files.readAllLines(Paths.get(fileName),
                    Charset.defaultCharset());
            for (String line : lines) {
            	String r[]=StrUtils.split(line, ",");
				int salt=generatetRowKeySalt(r,14,rNum);
				if(salt!=-1)
				{
					int counter=hm.get(salt);
					counter++;
					hm.put(salt, counter);
				}
				else
					cc++;
            }
            
            long et=System.currentTimeMillis();
            System.out.println("UsedTime:"+(et-st));
        } catch (IOException e) {
            e.printStackTrace();
        }
		 
	}
	
	
public static HashMap<String,Integer>hm2=new HashMap<String,Integer>();

	
	public static int generatetRowKeySalt(final String[] r,int idx, int rNum){
		// TODO Auto-generated method stub
		//[4][8][8][4]
		//String r[]=rowItems;
		

		
		try{
		String revMsisdn=r[idx];
		long msisdn;
			
		if(StrUtils.isBlankorZeroString(revMsisdn))//如果是空号码，那么生成一个随机的18位到数字
		{
			
			msisdn=(long)(Math.random()*1000000000000000000L);//生成一个随机到18位数字
			revMsisdn=StringUtils.rightPad(String.valueOf(msisdn), 18,"0");
			//补全到16位到长整数
			msisdn=Long.parseLong(revMsisdn);
		}
		else{
		//对小于11位的号码，如固话号码，左补0到11位（正常到msisdn位长)
		 revMsisdn=StringUtils.leftPad((new StringBuffer(r[idx])).reverse().toString(),11,"0");
		 //注意，有可能含有非数字字符,正常返回11位到long数字，当以下异常时，将取字符串hashcode，并返回固定位长的long
		 //1）revMsisdn可能包含字符,2)revMsisdn长度可能超过11位
		 //msisdn最长应该在11-15位间，返回16位右补长字段即可
		 msisdn=getAbsoluteLong(revMsisdn,16);
		}
		
		
		int salt=Math.abs(revMsisdn.hashCode())%rNum;
		if(salt==97)
		{
			//System.out.println(r[idx]);
			if(hm2.containsKey(r[idx]+"")){
				int counter=hm2.get(r[idx]);
				counter++;
				hm2.put(r[idx]+"", counter);
			}
			else
			{
				hm2.put(r[idx], 1);
			}
		}
			
	//	if(salt==142)
	//	{
	//		int counter=phone.get(revMsisdn);
	//		phone.put(revMsisdn, counter+1);
	//	}
		
		return salt;
	
		}catch(Exception e){
			//e.printStackTrace();
			
			//CLogger.logStackTrace(e);
			return -1;
		}
		
	}
	public static boolean isBlankString(final String str)
	{
		char chars[]=str.toCharArray();
		if(chars.length==0)
			return true;
		for(char c:chars)
		{
			//System.out.println(c!=' ');
			if(c!=' ')
				return false;
		}
		return true;
	}
	
	
	public static long getAbsoluteLong(String str,int fixLen)
	{
		try{
			return Long.parseLong(str);
		}catch(NumberFormatException e)
		{
			//返回18位的long数字,通过补全str到hashcode
			return Long.parseLong(StringUtils.rightPad(String.valueOf(Math.abs(str.hashCode())), fixLen,"0"));
		}
	}
}

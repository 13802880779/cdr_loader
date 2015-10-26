package CdrParser;

/**
 * 
 * @author root
 * @comment:定义索引对象到信息，包括索引名称、索引长度，索引与原cdr字段映射关系等
 */

public class IndexObj {
public String head="";
public int[] ctnLen;
public int[] colIdx;
public String[] dataType;
public int ctnNum=0;	


public IndexObj(String h,int[] cl,int[] ci,String[]dt)
{
	head=h;
	ctnLen=cl;
	colIdx=ci;
	dataType=dt;
	ctnNum=ctnLen.length;		
}
}
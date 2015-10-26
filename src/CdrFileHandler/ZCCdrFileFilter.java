package CdrFileHandler;

import java.io.File;
import java.io.FileFilter;

import CdrConfiguration.CConf;

/**
 * 
 * @author liangmeng
 * @usage 针对华为CS话单南向服务器目录结构专门设置到文件过滤器
 *
 */
public class ZCCdrFileFilter implements FileFilter{


	@Override
	public boolean accept(File pathname) {
		// TODO Auto-generated method stub

		
		return isValidCdrType(pathname);
	}
	
	private boolean isValidCdrType(File f)
	{
		//System.out.println("zcfile filter========>!"+f.getAbsolutePath());
		if(f.isDirectory())
			return true;
		
		//System.out.println("file filted!"+f.getParent());
		if(!f.getParent().endsWith("/bak"))
		{	
			//System.out.println("file filted!"+f.getAbsolutePath());
			return false;		
		}
		
		String[] ValidCdrTypes=CConf.getCdrFileSuffix();
		for(String s:ValidCdrTypes)
		{
			if(f.getName().endsWith("."+s))
				return isValidPrefix(f);
		}
		//System.out.println("return false");
		return false;
	}
	
	private boolean isValidPrefix(File f)
	{
		String firms[]=CConf.getCdrFirms();
		if(firms==null)
			return false;
		else
		{
			for(String firm:firms)
			{
				String prefixs[]=CConf.getCdrPrefix(firm);
				if(prefixs==null)
					return false;
				
				for(String prefix:prefixs)
				{
					if(f.getName().startsWith(prefix))
						return true;
				}
			}
		}
		return false;
		
	}
}
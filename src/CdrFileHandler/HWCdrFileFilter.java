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
public class HWCdrFileFilter implements FileFilter{


	@Override
	public boolean accept(File pathname) {
		// TODO Auto-generated method stub

		
		return isValidCdrType(pathname);
	}
	
	private boolean isValidCdrType(File f)
	{
		//System.out.println("file filter========>!");
		if(f.isDirectory())
			return true;
		
		
		if(!f.getParent().endsWith("/bak")|| f.getParent().endsWith("/CITY_860758/bak")||f.getParent().endsWith("/CITY_860760/bak"))
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
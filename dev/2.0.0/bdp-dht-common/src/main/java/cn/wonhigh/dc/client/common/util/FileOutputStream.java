package cn.wonhigh.dc.client.common.util;

import java.io.FileNotFoundException;

public class FileOutputStream extends java.io.FileOutputStream{

	public FileOutputStream(String name,String appendTime) throws FileNotFoundException {
		super(name);
	}

}

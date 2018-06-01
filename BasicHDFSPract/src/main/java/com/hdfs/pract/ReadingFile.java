package com.hdfs.pract;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;

public class ReadingFile {
	//Reading using URI
	static {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.99.100:9000");

		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory(conf));
	}

	public static void main(String[] args) throws MalformedURLException, IOException {
		InputStream in = null;
		try {
			in = new URL("hdfs:///user/exter/aa1.txt").openStream();
			IOUtils.copyBytes(in, System.out, 4096, false);

		} finally {
			IOUtils.closeStream(in);
		}
	}
}

package com.hdfs.pract;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

public class ReadingFileUsingFileAPI {

	public void readWithFile(String path) throws IOException {
		// It reads info from core-site.xml in resources directory
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		RemoteIterator<LocatedFileStatus> Rt = fs.listFiles(new Path(path), false);
		while (Rt.hasNext()) {
			String file = Rt.next().getPath().getName();
			System.out.println(file);
		}
	}

	public void readWithSchema(String path) throws IOException, URISyntaxException {
		// It reads info from core-site.xml in resources directory
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(path), conf);
//		RemoteIterator<LocatedFileStatus> Rt = fs.listFiles(new Path(path), false);
//		while (Rt.hasNext()) {
//			String file = Rt.next().getPath().getName();
//			System.out.println(file);
//		}
	}

	public static void main(String[] args) throws IOException, URISyntaxException {

		ReadingFileUsingFileAPI rfd = new ReadingFileUsingFileAPI();
		rfd.readWithSchema("hdfs://192.168.99.100:9000/user/exter");
	}

}

package cn.tao.hdfs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.GzipCodec;

public class SimpleUncompression {

	public static void main(String[] args) throws IOException {
		CompressionCodec codec=new GzipCodec();
		Configuration configuration=new Configuration();
		FileSystem fs=FileSystem.get(configuration);
		
		FSDataInputStream in=fs.open(new Path("D:\\program\\HDFSClient\\helloworld.gz"));
        CompressionInputStream cis=codec.createInputStream(in);
        byte[] buff=new byte[128];
        int length=0;
        while((length=cis.read(buff, 0, 128))!=-1){
        	System.out.println(new String(buff,0,length));
        }
	}

}

package cn.tao.smallfile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
/*
 * SequenceFile合并指定路径中的小文件
 */
public class SequenceFileWriteDemo2 {
  
  public static void main(String[] args) throws IOException {
    String uri = "/temp/events";
    Configuration conf = new Configuration();
    conf.set("fs.default.name", "hdfs://10.108.21.2:9000");
    FileSystem fs = FileSystem.get(URI.create(uri), conf);
    Path path = new Path(uri);
    Text key = new Text();
    Text value = new Text();
    File file = new File("D:\\events\\17-08-17\\");
    File[] listFiles = file.listFiles();
    SequenceFile.Writer writer = null;
    InputStream in = null;
    byte[] buff = null;
    try {
      writer = SequenceFile.createWriter(fs, conf, path,
          key.getClass(), value.getClass()); 
      int count=0;
      for (File f:listFiles) {
    	  in = new FileInputStream(f);
    	  int len=(int) f.length();
    	  buff = new byte[len];
    	  in.read(buff);
    	  
        key.set(f.getName()); 
        value.set(buff);
//        System.out.printf("[%s]\t%s\t%s\n", 
//        		writer.getLength(), key, value);
        writer.append(key, value);
      }
      
    } finally {
      IOUtils.closeStream(writer);
    }
  }
}

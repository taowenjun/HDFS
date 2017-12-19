package cn.tao.smallfile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;

/*
 * SequenceFile读取文件
 */
public class SequenceFileReadDemo {  
  public static void main(String[] args) throws IOException {
	long point1=System.currentTimeMillis();
	String uri = "/temp/events";
	//配置信息
	Configuration conf = new Configuration();
	conf.set("fs.default.name", "hdfs://10.108.21.2:9000");
	//创建FileSystem
    FileSystem fs = FileSystem.get(URI.create(uri), conf);
    Path path = new Path(uri);
    long point2=System.currentTimeMillis();
    long point3=0;
    long point4=0;
    //SequenceFile读取文件
    SequenceFile.Reader reader = null;
    try {
      reader = new SequenceFile.Reader(fs, path, conf);
      point3=System.currentTimeMillis();
      Writable key = (Writable)
        ReflectionUtils.newInstance(reader.getKeyClass(), conf);
      Writable value = (Writable)
        ReflectionUtils.newInstance(reader.getValueClass(), conf);
      long position = reader.getPosition();
      //File file=new File("D:\\output1.txt");
	 //OutputStream out=new FileOutputStream(file);
      while (reader.next(key, value)) {
      //同步记录的边界
    	//out.write((value.toString().getBytes()));
        String syncSeen = reader.syncSeen() ? "*" : "";
        System.out.printf("[%s%s]\t%s\t%s", position, syncSeen, key, value);
        position = reader.getPosition(); // beginning of next record
      }
     
      //out.close();
    } finally {
      IOUtils.closeStream(reader);
      point4=System.currentTimeMillis();
    }
    
    System.out.println("point2-point1:"+(point2-point1));
    System.out.println("point3-point2:"+(point3-point2));
    System.out.println("point4-point3:"+(point4-point3));
    System.out.println("total:"+(point4-point1));
  }
}
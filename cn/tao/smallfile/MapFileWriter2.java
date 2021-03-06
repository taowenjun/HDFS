package cn.tao.smallfile;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
/*
 * MapFile合并指定路径小文件
 */
public class MapFileWriter2 {
	//定义压缩的路径  
    private static final String URL = "hdfs://10.108.21.2:9000";      
    public static void main(String[] args) {            
        try {  
            //1.创建Configuration  
            Configuration conf = new Configuration();  
            //2.获取FileSystem  
            FileSystem fileSystem = FileSystem.get(new URI(URL), conf);  
            //3.创建文件输出路径Path  
            Path path = new Path(URL + "/mapfile/events");  
            //4.new一个MapFile.Writer对象  
            MapFile.Writer writer = new MapFile.Writer(conf, fileSystem, 
            		path.toString(), Text.class, Text.class);  
            //5.调用MapFile.Writer.append()方法追加写入 
            File file = new File("D:\\events\\17-08-17\\");
            File[] listFiles = file.listFiles();
            InputStream in = null;
            byte[] buff = null;
            for(File f:listFiles){
				in = new FileInputStream(f);
            	int len = (int)f.length();
            	buff = new byte[len];
            	in.read(buff);
            	writer.append(new Text(f.getName()), new Text(buff));              	
            }
            //关闭流  
            writer.close();  
        } catch (Exception e) {  
            // TODO: handle exception  
        }  
}
}
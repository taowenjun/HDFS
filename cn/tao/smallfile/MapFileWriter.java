package cn.tao.smallfile;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
/*
 * ʹ��MapFile�ϲ�С�ļ�
 */
public class MapFileWriter {
	//����ѹ����·��  
    private static final String URL = "hdfs://10.108.21.2:9000";      
    public static void main(String[] args) {            
        try {  
            //1.����Configuration  
            Configuration conf = new Configuration();  
            //2.��ȡFileSystem  
            FileSystem fileSystem = FileSystem.get(new URI(URL), conf);  
            //3.�����ļ����·��Path  
            Path path = new Path(URL + "/mapfile");  
            //4.newһ��MapFile.Writer����  
            MapFile.Writer writer = new MapFile.Writer(conf, fileSystem, 
            		path.toString(), Text.class, Text.class);  
            //5.����MapFile.Writer.append()����׷��д��  
            writer.append(new Text("name"), new Text("liaozhongmin"));  
            //�ر���  
            writer.close();  
        } catch (Exception e) {  
            // TODO: handle exception  
        }  
}
}
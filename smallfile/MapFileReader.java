package cn.tao.smallfile;

import java.net.URI;
import javax.swing.text.html.Option;
import javax.swing.text.AttributeSet;
import javax.swing.text.SimpleAttributeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MapFile;

import org.apache.hadoop.io.Text;
/*
 * MapFile��ȡ�ϲ�����ļ�
 */
public class MapFileReader {  	  
    //�����ļ���ȡ��·��  
    private static final String INPATH = "hdfs://10.108.21.2:9000";      
    public static void main(String[] args) {          
        try {  
        	long point1=System.currentTimeMillis();
            //����������Ϣ  
            Configuration conf = new Configuration();  
            long point2=System.currentTimeMillis();
            //�����ļ�ϵͳ  
            FileSystem fs = FileSystem.get(new URI(INPATH),conf);  
            //����Path����  
            long point3=System.currentTimeMillis();
            Path path = new Path(INPATH + "/mapfile/events");  
            //4.newһ��MapFile.Reader���ж�ȡ          
			MapFile.Reader reader = new MapFile.Reader(fs, path.toString(), conf); 
           // MapFile.Reader reader = new MapFile.Reader(path, conf, opts);
            long point4=System.currentTimeMillis();
            //����Key��Value  
            Text key = new Text();  
            Text  value = new Text();             
            //������ȡ���  
           // while (reader.next(key, value)){  
           //     System.out.println("key=" + key);  
           //     System.out.println("value=" + value);  
           // } 
            Text  v = new Text(); 
            reader.get(new Text("events-.1502977057500"), v);
            System.out.println(v);
            long point5=System.currentTimeMillis();
            //�ر���  
            IOUtils.closeStream(reader); 
            long point6=System.currentTimeMillis();
          //  long point7=System.currentTimeMillis();
            System.out.println("MapFile read a file");
            System.out.println("create conf:"+(point2-point1));
            System.out.println("create fs:"+(point3-point2));
            System.out.println("create reader:"+(point4-point3));
            System.out.println("read a file:"+(point5-point4));
            System.out.println("total:"+(point6-point1));
        } catch (Exception e) {  
            e.printStackTrace();  
        }  
          
    }  
}  

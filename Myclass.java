/*this program reads the data from HDFS*/
import org.apache.hadoop.conf.Configuration;
import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;





public class Myclass {

	public static void main(String[] args) throws IOException{
		String uri= "hdfs://localhost:54310/user/hadoop/new.txt"	;
		Configuration conf = new Configuration();
		FileSystem local = FileSystem.get(URI.create(uri),conf);
		FSDataInputStream in = null;
		System.out.println(local);
		try {
			
		in = local.open(new Path(uri));
		IOUtils.copyBytes(in, System.out, 4096, false);
		} finally {
		IOUtils.closeStream(in);
		}
	}
		
	}

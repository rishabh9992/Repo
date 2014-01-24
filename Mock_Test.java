/*test code for map reduce function*/
import static org.mockito.Mockito.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.OutputCollector;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;


public class Mock_Test {

	public static void main(String[] args) throws IOException {

		processesValidRecord();
		returnsMaximumIntegerInValues();
	}
	public static void processesValidRecord() throws IOException {
		@SuppressWarnings("resource")
		Max_Temp mapper = new Max_Temp();
		Text value = new Text("0043011990999991950051518004+68750+023550FM-12+0382" +
		// Year ^^^^
		"99999V0203201N00261220001CN9999999N9-00111+99999999999");
		// Temperature ^^^^^
		@SuppressWarnings("unchecked")
		OutputCollector<Text, IntWritable> output = mock(OutputCollector.class);
		mapper.map(null, value, output, null);
		verify(output).collect(new Text("1950"), new IntWritable(-11));
	}


	public static void returnsMaximumIntegerInValues() throws IOException {
	@SuppressWarnings("resource")
	Max_Red reducer = new Max_Red();
	Text key = new Text("1950");
	Iterator<IntWritable> values = Arrays.asList(new IntWritable(10), new IntWritable(5)).iterator();
	@SuppressWarnings("unchecked")
	OutputCollector<Text, IntWritable> output = mock(OutputCollector.class);
	reducer.reduce(key, values, output, null);
	verify(output).collect(key, new IntWritable(10));
	
	}


}




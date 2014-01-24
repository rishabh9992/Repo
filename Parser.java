

/*This class Parses the data from the HDFS*/
public class Parser {
	String Record,DN,type;
	public  Parser (String r){
			int a;
			Record=r;
			a=Record.indexOf('\t');
			DN=Record.substring(0, a);
			type=Record.substring(a);
			
			} 
	 
}

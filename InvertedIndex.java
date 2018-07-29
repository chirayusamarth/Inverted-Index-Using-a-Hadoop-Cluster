import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.util.logging.Logger;

public class InvertedIndex {
	private static final Logger log = Logger.getLogger(InvertedIndex.class.getName());
  public static class IndexMapper
       extends Mapper<Object, Text, Text, Text>{

    private Text word = new Text();
    private final static Text docid= new Text();
    
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      
      StringTokenizer itr = new StringTokenizer(value.toString());
      //if(itr.hasMoreTokens())
      		//docid.set(Integer.valueOf(itr.nextToken().toString()));
      String[] docvalue= value.toString().split("\t");
      String id= docvalue[0];
   	  docid.set(id);

      itr.nextToken();
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken().toString());
        context.write(word, docid);
      }
    }
  }

  public static class IndexReducer
       extends Reducer<Text,Text,Text,Text> {

    private Text result = new Text();

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
    	
      HashMap<String, Integer> hm = new HashMap<String, Integer>();
      for (Text v : values) {
        String docid = v.toString();
        if(hm!=null && hm.containsKey(docid))
        	hm.put(docid, hm.get(docid)+1);
        else 
        	hm.put(docid, 1);
      }

      String res = hm.toString();
      res= res.replace("{", "");
      res= res.replace("}", "");
      res= res.replace("=", ":");
      res= res.replace(",", "");
      res= res.replace(" ", "	");

      result.set(res);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Inverted Index");
    job.setJarByClass(InvertedIndex.class);
    job.setMapperClass(IndexMapper.class);
   // job.setCombinerClass(IndexReducer.class);
    job.setReducerClass(IndexReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
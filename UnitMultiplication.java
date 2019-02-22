import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

public class UnitMultiplication {

    public static class TransitionMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            //input format: fromPage\t toPage1,toPage2,toPage3
            //target: build transition matrix unit -> fromPage\t toPage=probability
            String line = value.toString().trim(); //Text to String, trim removes ths space
            String[] fromTo = line.split("\t");
            if(fromTo.length<=1 || fromTo[1].replaceAll("[^A-Za-z0-9_,]", "").trim().equals("") ) { //End if fromTo[1] doesn't contain any alphanumeric characters
                return;
            }

            String from=fromTo[0];
            String[] tos=fromTo[1].split(",");

            DecimalFormat df = new DecimalFormat("#.000000");
            double PR = Double.valueOf(df.format((double)1/tos.length));

            for(String to: tos){
                context.write(new Text(from), new Text(to+ "="+ PR) );
            }


        }
    }

    public static class PRMapper extends Mapper<Object, Text, Text, Text> {

        @Override  //?map is a method of Mapper, so now re-define it
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            //input format: Page\t PageRank
            //target: write to reducer
            String line=value.toString().trim();
            String[] keyPR=line.split("\t");
            if (keyPR.length<=1){
                return;
            }
            context.write(new Text(keyPR[0]), new Text(keyPR[1]));
        }
    }

    public static class MultiplicationReducer extends Reducer<Text, Text, Text, Text> {


        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            //input key = fromPage value=<toPage=probability..., pageRank>
            //target: get the unit multiplication
            double pr=0; //since it is a constant, Let's put it on the head.
            List<String> subStates=new ArrayList<String>();
            for (Text value: values){
                if (value.toString().contains("=") ){  //Java is the same with Python, need ()
                    subStates.add(value.toString().trim());
                }
                else{
                    pr= Double.parseDouble(value.toString().trim());//String to double I cannot declare pr in the loop or it will be a local variable.
                }
            }

            for(String substate: subStates){
                String[] elements=substate.split("=");
                double relation=Double.parseDouble(elements[1].trim());
                String outputPR= String.valueOf(pr*relation);
                context.write(new Text(elements[0]), new Text(outputPR) );
            }
        }
    }

    public static void main(String[] args) throws Exception {  //Define the main function to pass arguments.

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(UnitMultiplication.class);

        //how chain two mapper classes?
        ChainMapper.addMapper(job, TransitionMapper.class, Object.class, Text.class, Text.class, Text.class, conf); //Object: Captitalize the first letter.
        ChainMapper.addMapper(job, PRMapper.class, Object.class, Text.class, Text.class, Text.class, conf);
        job.setReducerClass(MultiplicationReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TransitionMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, PRMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.waitForCompletion(true);
    }

}

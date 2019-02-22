import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.text.DecimalFormat;

public class UnitSum {

    public static class PRMapper extends Mapper<Object, Text, Text, DoubleWritable>{

        float beta;
        @Override
        public void setup(Context context) {
            Configuration conf = context.getConfiguration();
            beta = conf.getFloat("beta", 0.2f);
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            //input Bi \t pr_i
            //output [Bi: beta*pr_i]  pass -pr-10 to distinguish from the subpr(>=0)
            String[] bi_pr= value.toString().trim().split("\t");
            if (bi_pr.length <=1){
                return;
            }

            String bi=bi_pr[0];
            double pr=Double.parseDouble(bi_pr[1]);
            context.write(new Text(bi), new DoubleWritable(beta*pr) );

        }
    }

    public static class PassMapper extends Mapper<Object, Text, Text, DoubleWritable> {

        float beta;
        @Override
        public void setup(Context context) {
            Configuration conf = context.getConfiguration();
            beta = conf.getFloat("beta", 0.2f);
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            //input format: toPage\t unitMultiplication [Bi; pr_ik]
            //target: pass to reducer
            //output: [Bi; (1-beta)* pr_ik]
            String subState=value.toString().trim();
            String[] tos=subState.split("\t");
            double subPR=Double.parseDouble(tos[1]); //It need to be a primitive to have the + - operation.
            context.write(new Text(tos[0]), new DoubleWritable((1-beta)*subPR) );
        }
    }



    public static class SumReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		
	    @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {

           //input key = toPage value = <unitMultiplication>
            //target: sum!
            double PR=0;
            for(DoubleWritable value: values){
                 PR = PR + value.get();
            }

            DecimalFormat df = new DecimalFormat("#.00000");
            PR = Double.valueOf(df.format(PR));
            context.write(key, new DoubleWritable(PR) );
         }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();  //Top 3
        conf.setFloat("beta", Float.parseFloat(args[3]));  
        Job job = Job.getInstance(conf);
        job.setJarByClass(UnitSum.class);


        //ChainMapper.addMapper(job, PRMapper.class, Object.class, Text.class, Text.class, DoubleWritable.class, conf);
        //ChainMapper.addMapper(job, PassMapper.class, Text.class, DoubleWritable.class, Text.class, DoubleWritable.class, conf); //Object: Captitalize the first letter.


        job.setMapperClass(PRMapper.class);
        job.setMapperClass(PassMapper.class);  //set up Mapper and Reducer class
        job.setReducerClass(SumReducer.class);

        job.setOutputKeyClass(Text.class);        //Set up the output key and value;
        job.setOutputValueClass(DoubleWritable.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, PRMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, PassMapper.class);//Set up the input and output path.
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.waitForCompletion(true);
    }
}

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Locale;

public class mm{

    public static class mm_mapper_1 extends Mapper<Object, Text, Text, Text>{
        String tags;
        int groups;
        int left_row_groups;
        int right_col_groups;
        int tasks;
        int left_rows;
        int right_cols;
        int left_cols;


        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            tags = conf.get("mm.tags", "ABC");
            groups = conf.getInt("mm.groups", 1);
            left_row_groups = conf.getInt("lr_groups",1);
            right_col_groups = conf.getInt("rc_groups",1);
            tasks = conf.getInt("mm.tasks", 1);
            left_rows = conf.getInt("left_rows", 0);
            left_cols = conf.getInt("left_cols", 0);
            right_cols = conf.getInt("right_cols", 0);

            super.setup(context);
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String s = value.toString();
            String[] tokens = s.split("\t");

            char left = tags.charAt(0);
            char right = tags.charAt(1);

            int row = Integer.parseInt(tokens[1]);
            int column = Integer.parseInt(tokens[2]);


            if (tokens[0].charAt(0) == left) {
                int I = row / (left_rows/left_row_groups);
                int J = column / (left_cols/groups);
                for (int K = 0; K < right_col_groups; K++) {
                    Text map_key = new Text(I+"\t" +J +"\t" +K);
                    Text map_value = new Text(tokens[0]+"\t"+row+"\t"+column+"\t"+Float.parseFloat(tokens[3]));
                    context.write(map_key, map_value);
                }
            } else if (tokens[0].charAt(0) == right) {
                int J = row / (left_cols/groups);
                int K = column / (right_cols/right_col_groups);
                for (int I = 0; I < left_row_groups; I++) {
                    Text map_key = new Text(I+"\t"+J+"\t"+K);
                    Text map_value = new Text(tokens[0]+"\t"+row+"\t"+column+"\t"+Float.parseFloat(tokens[3]));
                    context.write(map_key, map_value);
                }
            }
        }

    }

    public static class mm_reducer_1 extends Reducer<Text, Text, Text, Text> {
        int left_rows;
        int left_cols;
        int right_cols;
        String tags;
        int groups;
        int left_row_groups;
        int right_col_groups;
        int tasks;

        protected void setup(Reducer.Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            tags = conf.get("mm.tags", "ABC");
            groups = conf.getInt("mm.groups", 1);
            left_row_groups = conf.getInt("lr_groups",1);
            right_col_groups = conf.getInt("rc_groups",1);
            tasks = conf.getInt("mm.tasks", 1);
            left_rows = conf.getInt("left_rows", 0);
            left_cols = conf.getInt("left_cols", 0);
            right_cols = conf.getInt("right_cols", 0);
            super.setup(context);
        }

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            char left = tags.charAt(0);
            char right = tags.charAt(1);

            int left_row_temp = left_rows/left_row_groups;
            int left_col_temp = left_cols/groups;
            int right_col_tmp = right_cols/right_col_groups;

            float A[][] = new float[left_row_temp][left_col_temp];
            float B[][] = new float[left_col_temp][right_col_tmp];


            for (Text val : values) {
                String[] value_tokens = val.toString().split("\t");
                int row = Integer.parseInt(value_tokens[1]);
                int column = Integer.parseInt(value_tokens[2]);
                float value = Float.parseFloat(value_tokens[3]);
                if (value_tokens[0].charAt(0) == left) {
                    A[row%(left_row_temp)][column%(left_col_temp)] = value;
                } else {
                    B[row%(left_col_temp)][column%(right_col_tmp)] = value;
                }
            }

            String[] key_tokens = key.toString().split("\t");
            int I = Integer.parseInt(key_tokens[0]);
            int K = Integer.parseInt(key_tokens[2]);

            Float sum;
            for (int i = 0; i < left_row_temp; i++){
                int I_key = i+I*(left_row_temp);
                for (int k = 0; k < right_col_tmp; k++){
                    int K_key = k+K*(right_col_tmp);
                    Text out_key = new Text(I_key  + "\t" + K_key);
                    sum = new Float(0);
                    for (int j = 0; j < left_col_temp; j++){
                        sum += A[i][j]*B[j][k];
                    }
                    Text out_value = new Text(sum.toString());
                    context.write(out_key, out_value);
                }
            }
        }
    }


    public static class mm_mapper_2 extends Mapper<Text, Text, Text, Text> {

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
        }

        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            context.write(key, value);
        }
    }

    public static class mm_reducer_2 extends Reducer<Text, Text, Text, Text> {
        String float_format;
        char result;
        Formatter formatter;

        @Override
        protected void setup(Reducer.Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            result = conf.get("mm.tags","ABC").charAt(2);
            float_format = conf.get("mm.float-format", "%.3f");
            super.setup(context);
        }

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Float sum = new Float(0);

            for (Text value : values) {
                sum += Float.parseFloat(value.toString().split("\t")[0]);
            }
            String[] key_tokens = key.toString().split("\t");
            int i = Integer.parseInt(key_tokens[0]);
            int k = Integer.parseInt(key_tokens[1]);
            if (sum != 0.0){
                formatter = new Formatter(Locale.US);
                String s = i + "\t" + k + "\t" + formatter.format(float_format, sum);
                Text t = new Text(s);
                Text out_key = new Text(Character.toString(result));
                context.write(out_key, t);
            }
        }

    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        GenericOptionsParser ops = new GenericOptionsParser(conf, args);
        conf = ops.getConfiguration();

        int tasks = conf.getInt("mapred.reduce.tasks", 1);

        String left = otherArgs[otherArgs.length-3] + "/data";
        String right = otherArgs[otherArgs.length-2] + "/data";
        String result = otherArgs[otherArgs.length-1] + "/data";
        String tmp = otherArgs[otherArgs.length-1]+"/tmp";

        FileSystem fs = FileSystem.get(conf);

        Path res_dir = new Path(result);
        if (fs.exists(res_dir)){
            fs.delete(res_dir);
        }


        String left_size = otherArgs[otherArgs.length-3] + "/size";
        FSDataInputStream file_left = fs.open(new Path(left_size));
        BufferedReader left_reader = new BufferedReader(new InputStreamReader(file_left));
        String[] left_size_tokens = left_reader.readLine().split("\t");
        left_reader.close();

        int left_rows = Integer.parseInt(left_size_tokens[0]);
        int left_cols = Integer.parseInt(left_size_tokens[1]);
        conf.setInt("left_rows", left_rows);
        conf.setInt("left_cols", left_cols);

        String right_size = otherArgs[otherArgs.length-2] + "/size";
        FSDataInputStream file_right = fs.open(new Path(right_size));
        BufferedReader right_reader = new BufferedReader(new InputStreamReader(file_right));
        String[] right_size_tokens = right_reader.readLine().split("\t");
        right_reader.close();

        int right_rows = Integer.parseInt(right_size_tokens[0]);
        int right_cols = Integer.parseInt(right_size_tokens[1]);
        conf.setInt("right_rows", right_rows);
        conf.setInt("right_cols", right_cols);

        FSDataOutputStream outputStream = fs.create(new Path(otherArgs[otherArgs.length-1] +"/size"));
        outputStream.writeBytes(left_rows+"\t"+right_cols+"\n");
        outputStream.close();

        int groups = conf.getInt("mm.groups", 1);
        if (left_cols % groups != 0){
            for (int i = (int) Math.min(Math.round(Math.sqrt(left_cols)), groups); i > 0; i--){
                if (left_rows % i == 0) {
                    groups = i;
                    break;
                }
            }
        }
        conf.setInt("mm.groups", groups);

        int left_row_groups = conf.getInt("mm.lr_groups", groups);
        if (left_rows % left_row_groups != 0){
            for (int i = (int) Math.min(Math.round(Math.sqrt(left_rows)), left_row_groups); i > 0; i--){
                if (left_rows % i == 0) {
                    left_row_groups = i;
                    break;
                }
            }
        }
        conf.setInt("mm.lr_groups", left_row_groups);

        int right_col_groups = conf.getInt("mm.rc_groups", groups);
        if (right_cols % right_col_groups != 0){
            for (int i = (int) Math.min(Math.round(Math.sqrt(right_cols)), right_col_groups); i > 0; i--){
                if (right_cols % i == 0) {
                    right_col_groups = i;
                    break;
                }
            }
        }
        conf.setInt("mm.rc_groups", right_col_groups);

        Job job1 = Job.getInstance(conf);
        job1.setNumReduceTasks(tasks);
        job1.setJobName("mm1");
        job1.setJarByClass(mm.class);
        job1.setMapperClass(mm_mapper_1.class);
        job1.setReducerClass(mm_reducer_1.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        MultipleInputs.addInputPath(job1, new Path(left), TextInputFormat.class, mm_mapper_1.class);
        MultipleInputs.addInputPath(job1, new Path(right), TextInputFormat.class, mm_mapper_1.class);
        Path outputPath = new Path(tmp);
        SequenceFileOutputFormat.setOutputPath(job1, outputPath);
        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }


        Job job2 = Job.getInstance(conf);
        job2.setNumReduceTasks(tasks);
        job2.setJobName("mm2");
        job2.setJarByClass(mm.class);
        job2.setMapperClass(mm_mapper_2.class);
        job2.setReducerClass(mm_reducer_2.class);
        job2.setInputFormatClass(SequenceFileInputFormat.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.setInputFormatClass(SequenceFileInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        Path outputPath1=new Path(result);
        SequenceFileInputFormat.addInputPath(job2, outputPath);
        FileOutputFormat.setOutputPath(job2, outputPath1);
        boolean exit_code = job2.waitForCompletion(true);

        Path tmp_dir = new Path(tmp);
        fs.delete(tmp_dir);
        fs.close();
        
        System.exit(exit_code ? 0 : 1);
    }

}
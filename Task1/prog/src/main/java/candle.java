import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.Partitioner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Formatter;
import java.util.Locale;
import java.util.regex.Pattern;

public class candle {

    public static class K implements WritableComparable<K> {
        private String asset;
        private long start;

        protected void Writer(){

        }

        public K() {

        }

        public K (String a, long s) {
            this.asset = a;
            this.start = s;
        }

        @Override
        public int compareTo(K o) {
            String thisName = this.asset;
            long thisValue = this.start;
            String thatName = o.asset;
            long thatValue = o.start;
            if (thisValue > thatValue) {
                return 1;
            } else if (thisValue < thatValue) {
                return -1;
            } else {
                try{
                    return thisName.compareTo(thatName);
                } catch (NullPointerException n) {
                    return 1;
                }
            }
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeUTF(this.asset);
            dataOutput.writeLong(this.start);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            this.asset = dataInput.readUTF();
            this.start = dataInput.readLong();
        }
    }

    public static class V implements Writable {
        private long moment;
        private int id_deal;
        private float price_deal;

        public V() {

        }
        public V (long m, int i, float p) {
            this.moment = m;
            this.id_deal = i;
            this.price_deal = p;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeLong(this.moment);
            dataOutput.writeInt(this.id_deal);
            dataOutput.writeFloat(this.price_deal);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            this.moment = dataInput.readLong();
            this.id_deal = dataInput.readInt();
            this.price_deal = dataInput.readFloat();
        }
    }

    public static class Candle_Mapper extends Mapper<Object, Text, K, V> {

        int candle_width;
        String date_from;
        String mask;
        String date_to;
        String time_from;
        String time_to;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            candle_width = conf.getInt("candle.width", 300000);
            date_from = conf.get("candle.date.from", "19000101");
            date_to = conf.get("candle.date.to", "20200101");
            time_from = conf.get("candle.time.from", "1000");
            time_to = conf.get("candle.time.to", "1800");
            mask = conf.get("candle.securities", ".*");
            super.setup(context);
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String s = value.toString();
            String[] tokens = s.split(",");

            if (tokens[2].compareTo("MOMENT") != 0) {

                DateFormat format = new SimpleDateFormat("yyyyMMddHHmmssSSS", Locale.ENGLISH);
                Date date = new Date();
                String check = "#SYMBOL";
                Pattern pattern = Pattern.compile(mask);
                char[] cur_date=new char[8];
                tokens[2].getChars(0, 8, cur_date, 0);
                char[] cur_time=new char[9];
                tokens[2].getChars(8, 17, cur_time, 0);

                if (!tokens[0].equals(check) && pattern.matches(mask, tokens[0]) &&
                        (tokens[2].compareTo(date_from + time_from + "000") >= 0) &&
                        (String.valueOf(cur_date).compareTo(date_to) < 0) &&
                        (String.valueOf(cur_time).compareTo(time_to + "000") < 0)) {
                    try {
                        date = format.parse(tokens[2]);
                    } catch (ParseException e) {
                        e.printStackTrace();
                    }
                    long current = date.getTime();

                    K k = new K(tokens[0], current - (current % candle_width));
                    V v = new V(current, Integer.parseInt(tokens[3]), Float.parseFloat(tokens[4]));

                    context.write(k, v);
                }
            }

        }
    }


    public static class Candle_Reducer extends Reducer<K, V, NullWritable, Text> {

        Formatter formatter = new Formatter(Locale.US);
        private MultipleOutputs mos;

        protected void setup(Reducer.Context context) throws IOException, InterruptedException {
            mos = new MultipleOutputs(context);
            super.setup(context);
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            mos.close();
        }


        public void reduce(K key, Iterable<V> values, Context context) throws IOException, InterruptedException {
            float min_price = (float) 999999999;
            float max_price = (float) 0;
            float open_price = (float) 0;
            float close_price = (float) 0;
            long open_moment = 2000000000000000000L;
            long close_moment = 0;
            long id_open = 2000000000000000000L;
            long id_close = 0;

            for (V val : values) {
                if (val.price_deal > max_price) {
                    max_price = val.price_deal;
                }
                if (val.price_deal < min_price) {
                    min_price = val.price_deal;
                }
                if (val.moment < open_moment) {
                    open_moment = val.moment;
                    open_price = val.price_deal;
                } else if (val.moment == open_moment) {
                    if (val.id_deal < id_open) {
                        id_open = val.id_deal;
                        open_price = val.price_deal;
                    }
                }
                if (val.moment > close_moment) {
                    close_moment = val.moment;
                    close_price = val.price_deal;
                } else if (val.moment == open_moment) {
                    if (val.id_deal > id_close) {
                        id_close = val.id_deal;
                        close_price = val.price_deal;
                    }
                }

                Date start_candle = new Date(key.start);
                String start = new SimpleDateFormat("yyyyMMddHHmmssSSS", Locale.ENGLISH).format(start_candle);

            }

            Date start_candle = new Date(key.start);
            String start = new SimpleDateFormat("yyyyMMddHHmmssSSS", Locale.ENGLISH).format(start_candle);

            String s = key.asset + "," + start +"," + Float.toString(((float) Math.round(open_price*10))/10) + "," + Float.toString(((float) Math.round(max_price*10))/10) + "," + Float.toString(((float) Math.round(min_price*10))/10) + "," + Float.toString(((float) Math.round(close_price*10))/10);
            Text t = new Text(s);
            NullWritable tmp_key = NullWritable.get();
            mos.write(tmp_key, t, key.asset);
        }
    }

    public static class Candle_Partitioner extends Partitioner<K, V> {

        int candle_width;
        String start;

        protected void setup(Mapper.Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            start = conf.get("datefrom") + conf.get("timefrom") + "000";
            candle_width = conf.getInt("candle.width", 300000);
        }

        @Override
        public int getPartition(K k, V v, int i) {
            return Math.abs((Long.toString(k.start) + k.asset).hashCode() % i);
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        GenericOptionsParser ops = new GenericOptionsParser(conf, args);
        conf = ops.getConfiguration();
        int num_reducers = conf.getInt("candle.num.reducers", 1);
        Job job = Job.getInstance(conf);
        job.setNumReduceTasks(num_reducers);
        job.setJobName("candle");
        job.setJarByClass(candle.class);
        job.setMapperClass(Candle_Mapper.class);
        job.setPartitionerClass(Candle_Partitioner.class);
        job.setReducerClass(Candle_Reducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setMapOutputKeyClass(K.class);
        job.setMapOutputValueClass(V.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[otherArgs.length-2]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length-1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
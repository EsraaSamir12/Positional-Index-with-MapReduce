package ir_project_;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class mapper extends Mapper<LongWritable, Text, Text, Text> {
    private final Text Word = new Text();
    private final Text file_pos = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        FileSplit split = (FileSplit) context.getInputSplit();
        StringTokenizer tokenizer = new StringTokenizer(value.toString().toLowerCase());
        int pos = 0;
        while (tokenizer.hasMoreTokens()) {
            pos++;
            String fileName = split.getPath().getName().replace(".txt","");
            Word.set(tokenizer.nextToken());
            file_pos.set(fileName + ":" + pos);
            context.write(Word, file_pos);
        }
    }}

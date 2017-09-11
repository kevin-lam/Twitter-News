import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.fs.Path;

import java.io.BufferedWriter;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.io.IOException;

/**
 * Class that overrides TextOutputFormat. Used to output a JSON Object in the
 * reduce phase. 
 */
public class JsonOutputFormat extends TextOutputFormat<Text, NullWritable> {
  @Override
  public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext context) throws
      IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    Path path = getOutputPath(context);
    FileSystem fs = path.getFileSystem(conf);
    OutputStream out = fs.create(new Path(path, context.getJobName()));
    return new JsonRecordWriter(out);
  }

  private class JsonRecordWriter extends RecordWriter<Text, NullWritable> {
    boolean firstRecord = true;
    BufferedWriter writer;
    public JsonRecordWriter(OutputStream out) throws IOException {
      super();
      writer = new BufferedWriter(new OutputStreamWriter(out, "UTF-8"));
      writer.write("{\"terms\":[");
    }

    @Override
    public synchronized void write(Text text, NullWritable nullWritable) throws IOException {
      if (!firstRecord) {
        writer.write(",\n");
        firstRecord = false;
      }
      writer.write(text.toString());
      firstRecord = false;
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException {
      writer.write("]}");
      writer.close();
    }
  }
}

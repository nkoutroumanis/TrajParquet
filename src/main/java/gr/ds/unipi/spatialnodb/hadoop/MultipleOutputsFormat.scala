package gr.ds.unipi.spatialnodb.hadoop

import HadoopIO.MultipleOutputer
import HadoopIO.MultipleOutputer._
import org.apache.hadoop.io.{DataInputBuffer, NullWritable}
import org.apache.hadoop.mapred.RawKeyValueIterator
import org.apache.hadoop.mapreduce.counters.GenericCounter
import org.apache.hadoop.mapreduce.lib.output.{LazyOutputFormat, MultipleOutputs}
import org.apache.hadoop.mapreduce.task.ReduceContextImpl
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl.DummyReporter
import org.apache.hadoop.mapreduce.{Job, _}
import org.apache.hadoop.util.Progress

object MultipleOutputsFormat {
  // Type inference fails with this inlined in constructor parameters
  private def defaultMultipleOutputsMaker[K, V](io: TaskInputOutputContext[_, _, K, V]): MultipleOutputer[K, V] =
    new MultipleOutputs[K, V](io)
}
/**
 * Subclass this to create a multiple output creating OutputFormat. The subclass must have a nullary constructor so
 * hadoop can construct it with `.newInstance`, ugh...
 *
 * The output format expects a two-part key of the form (outputPath, actualKey), the string outputPath will be used to
 * partition the output into different directories ('/' separated filenames).
 *
 * For some reason MultipleOutputs does not work with Avro, but the near-identical AvroMultipleOutputs does. Irritatingly
 * these obviously related classes have no common ancestor so they are combined under the MultipleOutputer type class
 * which at least allows for future extension.
 *
 * @param outputFormat the underlying OutputFormat responsible for writing to varies
 * @param multipleOutputsMaker factory method for constructing an object implementing the MultiplerOutputer trait
 * @tparam K key type of the underlying OutputFormat
 * @tparam V value type of the underlying OutputFormat
 */
abstract class MultipleOutputsFormat[K, V]
(outputFormat: OutputFormat[K, V],
 multipleOutputsMaker: TaskInputOutputContext[_, _, K, V] => MultipleOutputer[K, V] =
 (r: TaskInputOutputContext[_, _, K, V]) => MultipleOutputsFormat.defaultMultipleOutputsMaker[K, V](r))
  extends OutputFormat[(String, K), V] {

  override def checkOutputSpecs(context: JobContext): Unit = outputFormat.checkOutputSpecs(context)

  override def getOutputCommitter(context: TaskAttemptContext): OutputCommitter = outputFormat
    .getOutputCommitter(context)

  override def getRecordWriter(context: TaskAttemptContext): RecordWriter[(String, K), V] =
    new RecordWriter[(String, K), V] {
      val job = Job.getInstance(context.getConfiguration)
      // Set underlying output format using lazy output
      LazyOutputFormat.setOutputFormatClass(job, outputFormat.getClass)
      // We pass a ReduceContext with most fields dummied-out since they will not be used in the context
      // of Spark's saveAs*Hadoop* methods
      val ioContext = new ReduceContextImpl(job.getConfiguration, context.getTaskAttemptID,
        new DummyIterator, new GenericCounter, new GenericCounter,
        new DummyRecordWriter, new DummyOutputCommitter, new DummyReporter, null,
        classOf[NullWritable], classOf[NullWritable])
      val multipleOutputs: MultipleOutputer[K, V] = multipleOutputsMaker(ioContext)

      override def write(keys: (String, K), value: V): Unit = {
        keys match {
          case (path, key) =>
            multipleOutputs.write(key, value, path)
        }
      }

      override def close(context: TaskAttemptContext): Unit = multipleOutputs.close()
    }


  private class DummyOutputCommitter extends OutputCommitter {
    override def setupJob(jobContext: JobContext): Unit = ()
    override def needsTaskCommit(taskContext: TaskAttemptContext): Boolean = false
    override def setupTask(taskContext: TaskAttemptContext): Unit = ()
    override def commitTask(taskContext: TaskAttemptContext): Unit = ()
    override def abortTask(taskContext: TaskAttemptContext): Unit = ()
  }

  private class DummyRecordWriter extends RecordWriter[K, V] {
    override def write(key: K, value: V): Unit = ()
    override def close(context: TaskAttemptContext): Unit = ()
  }

  private class DummyIterator extends RawKeyValueIterator {
    override def getKey: DataInputBuffer = null
    override def getValue: DataInputBuffer = null
    override def getProgress: Progress = null
    override def close(): Unit = ()
    override def next: Boolean = true
  }

}
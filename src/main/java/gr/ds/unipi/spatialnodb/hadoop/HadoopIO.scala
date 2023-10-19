package gr.ds.unipi.spatialnodb.hadoop

import org.apache.avro.mapreduce.AvroMultipleOutputs
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs

object HadoopIO {

  /**
   * This is a work-around for the fact that Avro has it's own non-type-compatible version of MultipleOutputs
   * it also allows for extending the functionality of MultipleOutputsFormat for other multiple output writers by
   * providing this missing interface as a trait.
   */
  trait MultipleOutputer[K, V] {
    def write(key: K, value: V, path: String): Unit
    def close(): Unit
  }

  object MultipleOutputer {

    implicit class AvroMultipleOutputer[K, V](mo: AvroMultipleOutputs) extends MultipleOutputer[K, V] {
      def write(key: K, value: V, path: String): Unit = mo.write(key, value, path)
      def close(): Unit = mo.close()
    }

    implicit class PlainMultipleOutputer[K, V](mo: MultipleOutputs[K, V]) extends MultipleOutputer[K, V] {
      def write(key: K, value: V, path: String): Unit = mo.write(key, value, path)
      def close(): Unit = mo.close()
    }

  }

}

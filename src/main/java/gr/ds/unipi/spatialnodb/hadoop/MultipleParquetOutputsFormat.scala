package gr.ds.unipi.spatialnodb.hadoop

import org.apache.avro.generic.GenericContainer
import org.apache.parquet.hadoop.ParquetOutputFormat

class MultipleParquetOutputsFormat[T <: GenericContainer]
  extends MultipleOutputsFormat (new ParquetOutputFormat[T]) {
}

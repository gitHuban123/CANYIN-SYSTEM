package cn.qm.etl2

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext}
import org.apache.hadoop.util.LineReader

class MyRecordReader extends RecordReader[LongWritable,Text]{
  var start, end, position = 0L
  var reader: LineReader = null
  var key = new LongWritable
  var value = new Text

  override def getCurrentKey: LongWritable = ???

  override def getProgress: Float = ???

  override def nextKeyValue(): Boolean = ???

  override def getCurrentValue: Text = ???

  override def initialize(inputSplit: InputSplit, taskAttemptContext: TaskAttemptContext): Unit = {
    start = 0.max()

  }

  override def close(): Unit = ???
}

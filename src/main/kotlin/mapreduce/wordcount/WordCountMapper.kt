package com.vifeng.hadoopstarter.mapreduce.wordcount

import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper

class WordCountMapper : Mapper<LongWritable, Text, Text, LongWritable>() {
    val one = LongWritable(1)

    override fun map(key: LongWritable?, value: Text?, context: Context?) {
        val line = value?.toString()
        val words = line?.split(" ")
        words?.forEach({ word ->
            context?.write(Text(word), one)
        })
    }
}
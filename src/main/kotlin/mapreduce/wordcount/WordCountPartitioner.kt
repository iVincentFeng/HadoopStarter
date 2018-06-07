package com.vifeng.hadoopstarter.mapreduce.wordcount

import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Partitioner

class WordCountPartitioner: Partitioner<Text, LongWritable>() {
    override fun getPartition(p0: Text?, p1: LongWritable?, p2: Int): Int {
        return 0
    }
}
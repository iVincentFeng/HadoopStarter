package com.vifeng.hadoopstarter.mapreduce.wordcount

import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Reducer

class WordCountReducer : Reducer<Text, LongWritable, Text, LongWritable>() {
    override fun reduce(key: Text?, values: MutableIterable<LongWritable>?, context: Context?) {
        if ((key == null) || (values == null)) {
            return
        }

        var sum = 0L
        for (value in values) {
            sum += value.get()
        }

        context?.write(key, LongWritable(sum))
    }
}
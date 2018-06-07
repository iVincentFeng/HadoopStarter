package com.vifeng.hadoopstarter.mapreduce.wordcount

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat


class WordCountMain {
    companion object {
        @JvmStatic fun main(args: Array<String>) {
            val config = Configuration()

            val job = Job.getInstance(config, "vifeng_wordcount")
            job.setJarByClass(WordCountMapper::class.java)

            FileInputFormat.setInputPaths(job, Path(args[0]))

            job.mapperClass = WordCountMapper::class.java
            job.mapOutputKeyClass = Text::class.java
            job.mapOutputValueClass = LongWritable::class.java

            job.combinerClass = WordCountReducer::class.java

            job.partitionerClass = WordCountPartitioner::class.java
            job.numReduceTasks = 4

            job.reducerClass = WordCountReducer::class.java

            job.outputKeyClass = Text::class.java
            job.outputValueClass = LongWritable::class.java

            FileOutputFormat.setOutputPath(job, Path(args[1]))

            job.waitForCompletion(true)
        }
    }

}

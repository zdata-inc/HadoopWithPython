import luigi
import luigi.contrib.hadoop
import luigi.contrib.hdfs

class InputFile(luigi.ExternalTask):
   """
   A task wrapping the HDFS target
   """
   input_file = luigi.Parameter()

   def output(self):
      """
      Return the target on HDFS
      """
      return luigi.contrib.hdfs.HdfsTarget(self.input_file)

class WordCount(luigi.contrib.hadoop.JobTask):
   """
   A task that uses Hadoop streaming to perform WordCount
   """
   input_file = luigi.Parameter()
   output_file = luigi.Parameter()

   # Set the number of reduce tasks
   n_reduce_tasks = 1

   def requires(self):
      """
      Read from the output of the InputFile task
      """
      return InputFile(self.input_file)

   def output(self):
      """
      Write the output to HDFS
      """
      return luigi.contrib.hdfs.HdfsTarget(self.output_file)

   def mapper(self, line):
      """
      Read each line and produce a word and 1
      """
      for word in line.strip().split():
         yield word, 1

   def reducer(self, key, values):
      """
      Read each word and produce the word and the sum of it's values
      """
      yield key, sum(values)

if __name__ == '__main__':
   luigi.run(main_task_cls=WordCount)
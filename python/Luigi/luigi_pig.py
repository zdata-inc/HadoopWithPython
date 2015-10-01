import luigi
import luigi.contrib.pig
import luigi.contrib.hdfs

class InputFile(luigi.ExternalTask):
   """
   A task wrapping the HDFS target
   """
   input_file = luigi.Parameter()

   def output(self):
      return luigi.contrib.hdfs.HdfsTarget(self.input_file)

class WordCount(luigi.contrib.pig.PigJobTask):
   """
   A task that uses Pig to perform WordCount
   """
   input_file = luigi.Parameter()
   output_file = luigi.Parameter()
   script_path = luigi.Parameter(default='../../pig/wordcount.pig')

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

   def pig_parameters(self):
      """
      A dictionary of parameters to pass to pig
      """
      return {'INPUT': self.input_file, 'OUTPUT': self.output_file}

   def pig_options(self):
      """
      A list of options to pass to pig
      """
      return ['-x', 'mapreduce']

   def pig_script_path(self):
      """
      The path to the pig script to run
      """
      return self.script_path

if __name__ == '__main__':
   luigi.run(main_task_cls=WordCount)
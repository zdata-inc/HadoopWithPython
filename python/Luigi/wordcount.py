import luigi

class InputFile(luigi.Task):
   """
   A task wrapping a Target 
   """
   input_file = luigi.Parameter()

   def output(self):
      """
      Return the target for this task
      """
      return luigi.LocalTarget(self.input_file)

class WordCount(luigi.Task):
   """
   A task that counts the number of words in a file
   """
   input_file = luigi.Parameter()
   output_file = luigi.Parameter(default='/tmp/wordcount')

   def requires(self):
      """
      The task's dependencies:
      """
      return InputFile(self.input_file)

   def output(self):
      """
      The task's output
      """
      return luigi.LocalTarget(self.output_file)

   def run(self):
      """
      The task's logic
      """
      count = {}

      ifp = self.input().open('r')

      for line in ifp:
         for word in line.strip().split():
            count[word] = count.get(word, 0) + 1

      ofp = self.output().open('w')
      for k, v in count.items():
            ofp.write('{}\t{}\n'.format(k, v))
      ofp.close()

if __name__ == '__main__':
   luigi.run()
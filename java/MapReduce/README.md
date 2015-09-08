# MapReduce

A repository full of sample MapReduce code.

## Requirements

A good e-book is all that is required for these examples. A popular place to get them is [Project GutenBerg](http://www.gutenberg.org/ebooks/2701).

Once you have a book or two, they must be placed in an input directory on HDFS for these examples to work.

## Projects

This is a listing of all the projects and some information about them.

### WordCount

This is the classic MapReduce example that counts the frequency of words in a book. The input for the Mapper are the lines in text document. The mapper takes each line and splits it into words based on spaces. The Mapper then outputs a word as the key and the value '1' as it't output value.

The Reducer takes each word and sums up the values for each word. This results in the ouptut having the format "word    count".

To speed things up, the Reducer is used as a Combiner that runs on the output of each Mapper before the results is sent to the Reducer. The Combiner is used to computer partial word counts for words to relieve the amount of data sent and processing needed by the Reducer. 

#### Building

To build this program use the following Maven command from within the WordCount directory:

    mvn clean install

This will build the project and place the jar in the `target` directory.

#### Running

The Word Count application takes two arguments, _inputDirectory_ and _outputDirectory_. Running the following command from the command line will reaveal these arguments:

    $ hadoop jar target/MapReduce-1.0-SNAPSHOT.jar com.zachradtka.mapreduce.WordCount <inputDirectory> <ouputDirectory>

The following example command will run WordCount and place the ouput in a file called `part-r-00000` in the directory `output42` in HDFS.

    $ hadoop jar target/MapReduce-1.0-SNAPSHOT.jar com.zachradtka.mapreduce.WordCount input/books output42

To view the output the _hadoop cat_ command can be used.

    $ hadoop fs -cat output42/part*

Be careful. Depending on the size of text input, this will easily run off the screen so you might want to pipe this to less. 

One final note, displaying the output using _cat_ orders the output by alphabetical order on the keys or words, not by the values or word count. This occurs because of how shuffle/sort works in MapReduce. To display the words in order of frequency, the following command can be used.

    $ hadoop fs -cat output42/part* | sort -k2 -n -r | less

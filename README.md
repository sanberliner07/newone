Project Hello_fresh_task
(Due Aug 10, morning, no late submissions)
Hello_fresh_task focuses on using Apache Spark for doing large-scale data analysis tasks. 
For this assignment, I use relatively small dataframe and I won't run anything in distributed mode; 
however Spark can be easily used to run the same programs on much larger datasets.

Getting Started with Spark
This guide is basically a summary of the excellent tutorials that can be found at the Spark website.
Apache Spark is a relatively new cluster computing framework, developed originally at UC Berkeley. It significantly generalizes
the 2-stage Map-Reduce paradigm (originally proposed by Google and popularized by open-source Hadoop system); Spark is instead based on the abstraction of resilient distributed datasets (RDDs). An RDD is basically a distributed collection
of items, that can be created in a variety of ways. Spark provides a set of operations to transform one or more RDDs into an output RDD.
Immutable in nature : We can create DataFrame / RDD once but can’t change it.

In Apache Spark, a DataFrame is a distributed collection of rows under named columns. In simple terms, it is same as a table in relational database or an Excel sheet with Column headers. It also shares some common characteristics with RDD:
and analysis tasks are written aschains of the dataframe  operations.
Spark can be used with the Hadoop ecosystem, including the HDFS file system and the YARN resource manager.

Installing Hello_fresh_task
Download the repository as git clone https://github.com/hellofreshdevtests/sanberliner07-data-engineering-test
We are ready to use Spark.

Spark and Python
Spark primarily supports three languages: Scala (Spark is written in Scala), Java, and Python. I use Python here -- you can follow the instructions at the tutorial
and quick start (http://spark.apache.org/docs/latest/quick-start.html) for other languages. The Java equivalent code can be very verbose and hard to follow. The below
shows a way to use the Python interface through the standard Python shell.

Anaconda3 Spyder 
To use Spark within the Spyder , you can also use it through Anaconda.


Anaconda prompt 
You can also use the Anaconda Shell.


$SPARKHOME/bin/pyspark: This will start a Python shell (it will also output a bunch of stuff about what Spark is doing). The relevant variables are initialized in this python
shell, but otherwise it is just a standard Python shell.


>>> textfile = sc.textFile("README.md"): This creates a new RDD, called textfile, by reading data from a local file. The sc.textFile commands create an RDD
containing one entry per line in the file.


You can see some information about the Dataframe  by doingspark = SparkSession.builder.master


I recommend you follow the rest of the commands in the quick start guide (http://spark.apache.org/docs/latest/quick-start.html). Here I will simply do the Hello fresh application.


Hello Fresh Application
The following command (in the pyspark shell) does a beef_ingredients and df_avg_cooking_time, i.e., it gives ingredients beef present and avg_total_cooking_time appears in the file README.md. Use beef_ingredients.show() and df_avg_cooking_time.show() to see the output.
>>> df_avg_cooking_time = df_difficulity_level.groupBy("difficulity").avg("total_cook_time")
>>> beef_ingredients = df_recipe.where(col('ingredients').like("%beef%"))

Here is the same code without the use of lambda functions.

df_avg_cooking_time = df_difficulity_level.groupBy("difficulity").avg("total_cook_time")
>>> beef_ingredients = df_recipe.where(col('ingredients').like("%beef%"))

Running it as an Application
Instead of using a shell, you can also write your code as a python file, and submit that to the spark cluster. The project5 directory contains a python file wordcount.py,
which runs the program in a local mode. To run the program, do:
$SPARKHOME/bin/\spark\bin\spark-submit hello_fresh_task_latest.py "C:/Users/Windows 10/OneDrive/Desktop/sanberliner07-data-engineering-test/input/recipes-000.json" "C:/Users/Windows 10/OneDrive/Desktop/sanberliner07-data-engineering-test/input/recipes-001.json" "C:/Users/Windows 10/OneDrive/Desktop/sanberliner07-data-engineering-test/input/recipes-002.json" "C:/SparkCourse/csvcs3.csv" "C:/SparkCourse/csvcs78.csv"


More...
We encourage you to look at the Spark Programming Guide and play with the other Dataframe manipulation commands.
You should also try out the Scala and Java interfaces.

Assignment Details
We have provided a Python file: assignment.py, that initializes the folllowing Dataframes:

An Dataframe consisting of lines from a 3 Recipes input data.(recipes-000.json. recipes-001.json, recipes-002.json)
An Dataframe consisting of lines from a Ingredients of beef data.
An Dataframe consisting of 2-tuples indicating Difficulity level and Avg(total_cooking_time) from all 3 recipes input data.


Your tasks are to fill out the functions that are defined in the hello_fresh_task3_latestt.py file (starting with task). The amount of code that you write would typically be small.


Task 1: This takes as input from recipes json and for each line, creates the 3 dataframes and merge them and removes the duplicate and the finds the data where ingredients are as "beef".

beef_ingredients = df_recipe.where(col('ingredients').like("%beef%"))
            
beef_ingredients.show()


Task 2 :find the difficullty level as described in the requirenment and then find the avg of total cooking time.

df_avg_cooking_time = df_difficulity_level.groupBy("difficulity").avg("total_cook_time")
df_avg_cooking_time.show()

Sample beef_ingredients.txt File

df_avg_cooking_time.txt File

Spark-submit arguments-

3 input file parameter
 input_data1 = sys.argv[1]
 input_data2 = sys.argv[2]
 input_data3 = sys.argv[3]
2 output files arguments
 output_data1 = sys.argv[4]
 output_data2 = sys.argv[5]

You can use spark-submit to run the hello_fresh_task3_latestt.py file, but it would be easier to develop with pyspark (by copying the commands over). We will also shortly post iPython instructions.
Sample beef_ingredients.txt and df_avg_cooking_time.txt Files show the results of running hello_fresh_task3_latestt.py on our code using: $ c:\SparkCourse>c:\spark\bin\spark-submit hello_fresh_task_latest.py "C:/Users/Windows 10/OneDrive/Desktop/sanberliner07-data-engineering-test/input/recipes-000.json" "C:/Users/Windows 10/OneDrive/Desktop/sanberliner07-data-engineering-test/input/recipes-001.json" "C:/Users/Windows 10/OneDrive/Desktop/sanberliner07-data-engineering-test/input/recipes-002.json" "C:/SparkCourse/csvcs3.csv" "C:/SparkCourse/csvcs78.csv"

Submission
Submit the hello_fresh_task3_latestt.py file here.
import os
import sys

# Fix encoding for Windows PowerShell
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# Set Python executable for Spark workers
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession

# Initialize a SparkSession
spark = SparkSession.builder.appName("SimpleDataFrame").getOrCreate()

# Create a DataFrame
data = [("John", 28), ("Smith", 44), ("Adam", 65), ("Henry", 23)]
columns = ["Name", "Age"]
df = spark.createDataFrame(data, columns)

# Show the original DataFrame
df.show()

# filter rows where the age is greater than 30
df.filter(df.Age > 30)

# Show the transformed DataFrame
df.show()

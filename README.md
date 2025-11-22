# Data Processing with PySpark - Lab Repository

This repository contains comprehensive PySpark labs covering fundamental to advanced data processing concepts.

## 📋 Table of Contents
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Lab Overview](#lab-overview)
- [Running the Labs](#running-the-labs)
- [Lab Details](#lab-details)
- [Troubleshooting](#troubleshooting)

## 🔧 Prerequisites

- **Python 3.8+** (Python 3.11 recommended)
- **Java 8 or 11** (required by Spark)
- **Git** (for cloning and version control)
- **4GB RAM minimum** (8GB recommended)

## 📥 Installation

### 1. Clone the Repository
```bash
git clone https://github.com/dicar-2001/Data-Processing-.git
cd Data-Processing-
```

### 2. Create Virtual Environment
```powershell
# Windows PowerShell
python -m venv venv
```

### 3. Activate Virtual Environment
```powershell
# Windows PowerShell
.\venv\Scripts\Activate.ps1

# If you get an execution policy error, run:
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

### 4. Install Dependencies
```powershell
pip install pyspark==3.5.3
```

### 5. Verify Installation
```powershell
python -c "import pyspark; print(f'PySpark {pyspark.__version__} installed successfully')"
```

## 📚 Lab Overview

| Lab | Title | Concepts Covered | Difficulty |
|-----|-------|------------------|------------|
| **LAB 1** | Hello Spark | SparkSession, DataFrames, Basic Operations | ⭐ Beginner |
| **LAB 2** | Data Loading & Transformations | File I/O, Schema, Missing Data, Type Conversion | ⭐⭐ Intermediate |
| **LAB 3** | Advanced Operations | Joins, Window Functions, UDFs, Aggregations | ⭐⭐⭐ Advanced |

## 🚀 Running the Labs

### Quick Start
```powershell
# Activate virtual environment (if not already active)
.\venv\Scripts\Activate.ps1

# Run any lab
python lab1_hello_spark.py
python lab2_data_loading.py
python lab3_advanced_operations.py
```

### Expected Output
Each lab will:
- ✅ Display step-by-step progress
- 📊 Show sample data and results
- ⚠️ Display warnings (Hadoop on Windows is normal)
- ✓ Confirm successful completion

## 📖 Lab Details

### LAB 1: Hello Spark (First Application)
**File:** `lab1_hello_spark.py`

**Topics:**
- SparkSession initialization
- Creating DataFrames from static data
- Schema inspection
- Basic transformations: `filter()`, `select()`, `groupBy()`, `orderBy()`
- Actions: `show()`, `count()`, `collect()`
- Aggregations: `avg()`, `count()`, `sum()`, `min()`, `max()`

**Key Operations:**
```python
# Filter products by category
laptops = products_df.filter(col("category") == "Laptop")

# Group by category and calculate statistics
category_stats = products_df.groupBy("category").agg(...)

# Order by price descending
sorted_products = products_df.orderBy(col("price").desc())
```

**Sample Output:**
- Product catalog (20 items)
- Laptop filtering results
- Category statistics (count, avg price, total inventory)
- Price analysis with sorting

---

### LAB 2: Data Loading & Basic Transformations
**File:** `lab2_data_loading.py`

**Topics:**
- Creating sample datasets
- Schema inspection and data profiling
- Column selection and filtering
- Adding computed columns with `withColumn()`
- Handling missing data: `fillna()`, `dropna()`
- Type conversions: `cast()`
- String operations: `upper()`, `lower()`, `concat_ws()`
- Sorting and aggregation
- Working with distinct values
- Column renaming
- Data sampling
- File format conversions (CSV, JSON, Parquet)*

**Key Operations:**
```python
# Add computed columns
df = df.withColumn("total_price", col("price") * col("quantity"))

# Handle missing data
df_filled = df.fillna({"discount": 0.0, "notes": "N/A"})

# Type conversion
df = df.withColumn("product_id", col("product_id").cast(IntegerType()))

# Aggregations
summary = df.groupBy("category").agg(
    count("*").alias("total_products"),
    spark_round(avg("price"), 2).alias("avg_price")
)
```

**Sample Dataset:** E-commerce data (15 products with categories, prices, inventory)

**Note:** *Steps 14-15 (file writing) may fail on Windows due to Hadoop compatibility issues. This is expected and documented in the lab.

---

### LAB 3: Advanced Operations
**File:** `lab3_advanced_operations.py`

**Topics:**
1. **JOIN Operations**
   - INNER JOIN: Matching records only
   - LEFT JOIN: All left + matched right
   - RIGHT JOIN: All right + matched left
   - FULL OUTER JOIN: All records from both
   - CROSS JOIN: Cartesian product

2. **Window Functions**
   - `row_number()`: Sequential numbering
   - `rank()` & `dense_rank()`: Ranking with/without gaps
   - `lag()` & `lead()`: Previous/next values
   - Cumulative sums: Running totals
   - Moving averages: Sliding window calculations
   - `ntile()`: Divide into percentiles
   - `first()` & `last()`: Boundary values

3. **User-Defined Functions (UDFs)**
   - Simple categorization UDF
   - Multi-parameter UDF
   - String manipulation UDF

4. **Advanced Aggregations**
   - Multiple aggregations per group
   - Conditional aggregations
   - Pivot tables

5. **Complex Transformations**
   - Nested `when-otherwise` chains
   - Array operations with `explode()`
   - Struct creation and access

6. **Set Operations**
   - `union()`: Combine datasets
   - `intersect()`: Common records
   - `except()`: Difference

7. **Real-World Case Study**
   - Customer segmentation (VIP, Premium, Regular, New)
   - Lifetime value scoring
   - Data quality validation

**Key Operations:**
```python
# Window function example
from pyspark.sql.window import Window

window_spec = Window.partitionBy("customer_id").orderBy(col("order_date").desc())
df_ranked = df.withColumn("row_num", row_number().over(window_spec))

# UDF example
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

categorize_udf = udf(lambda amount: "High" if amount > 300 else "Low", StringType())
df = df.withColumn("category", categorize_udf(col("amount")))

# Pivot table
pivot_df = df.groupBy("customer_id").pivot("status").count()
```

**Sample Datasets:**
- Customers (5 records)
- Orders (10 records)
- Products (5 records)

---

## 🛠️ Troubleshooting

### Common Issues

#### 1. Virtual Environment Not Activating
```powershell
# Error: Scripts cannot be loaded because running scripts is disabled
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser

# Then try again
.\venv\Scripts\Activate.ps1
```

#### 2. Java Not Found
```bash
Error: JAVA_HOME is not set
```
**Solution:** Install Java 8 or 11 and set JAVA_HOME environment variable.

#### 3. Hadoop Warnings on Windows
```
WARN Shell: Did not find winutils.exe
```
**Solution:** This is normal on Windows. The labs run successfully despite this warning.

#### 4. Path with Spaces Issue
```
'HP' is not recognized as a command
```
**Solution:** This happens due to spaces in username. The labs handle this automatically with proper quoting.

#### 5. Python Worker Connection Timeout
```
Caused by: java.net.SocketTimeoutException: Accept timed out
```
**Solution:** This is intermittent. Simply re-run the lab. The environment usually stabilizes after first run.

#### 6. Port Already in Use
```
Address already in use: bind
```
**Solution:** Change the port in the lab file:
```python
.config("spark.ui.port", "4041")  # Try different port
```

### Windows-Specific Notes

- **File Writing (LAB2 Steps 14-15):** May fail due to Hadoop/Windows compatibility. This is documented and expected.
- **Encoding Issues:** Labs automatically configure UTF-8 encoding for PowerShell.
- **Path Handling:** Python executable paths are set automatically to avoid worker issues.

## 📊 Lab Execution Times

| Lab | Typical Duration | Dataset Size |
|-----|-----------------|--------------|
| LAB 1 | ~10-15 seconds | 20 records |
| LAB 2 | ~15-20 seconds | 15 records |
| LAB 3 | ~6-8 minutes | 20 records (multiple datasets) |

**Note:** LAB 3 takes longer due to complex window operations and multiple transformations.

## 📁 Repository Structure

```
data_processing/
├── venv/                          # Virtual environment (not in git)
├── LAB_0_A.ipynb                 # Jupyter notebook (LAB 0A)
├── LAB_0_B.ipynb                 # Jupyter notebook (LAB 0B)
├── lab1_hello_spark.py           # LAB 1: First Spark Application
├── lab2_data_loading.py          # LAB 2: Data Loading & Transformations
├── lab3_advanced_operations.py   # LAB 3: Advanced Operations
├── LAB1.pdf                      # LAB 1 instructions
├── LAB2.pdf                      # LAB 2 instructions
├── LAB3.pdf                      # LAB 3 instructions
├── output/                       # Generated output files (if any)
├── data-engineering-course/      # Docker Spark cluster config
│   └── docker-compose.yml
└── README.md                     # This file
```

## 🎓 Learning Path

**Recommended Order:**
1. ✅ **LAB 1** - Get familiar with Spark basics
2. ✅ **LAB 2** - Learn data loading and transformations
3. ✅ **LAB 3** - Master advanced operations

**Each lab builds on concepts from previous labs.**

## 🔍 Key Concepts Reference

### DataFrame Operations
```python
# Selection
df.select("col1", "col2")
df.selectExpr("col1", "col2 * 2 as doubled")

# Filtering
df.filter(col("price") > 100)
df.where((col("category") == "Electronics") & (col("stock") > 0))

# Adding columns
df.withColumn("new_col", col("old_col") * 2)

# Renaming
df.withColumnRenamed("old_name", "new_name")

# Aggregation
df.groupBy("category").agg(count("*"), avg("price"))

# Sorting
df.orderBy(col("price").desc())
```

### Important Functions
```python
from pyspark.sql.functions import (
    col, lit, when, avg, sum, count, min, max,
    upper, lower, concat, concat_ws, substring,
    year, month, dayofmonth, datediff,
    row_number, rank, dense_rank, lag, lead,
    explode, array, struct
)
```

## 💡 Best Practices

1. **Always activate virtual environment** before running labs
2. **Check output carefully** - each step shows verification results
3. **Don't worry about warnings** - Hadoop warnings on Windows are normal
4. **Re-run if timeout occurs** - Python worker issues resolve on retry
5. **Read step descriptions** - each step explains what it demonstrates
6. **Experiment** - modify code to test understanding

## 📞 Support

For issues or questions:
- **Repository:** [github.com/dicar-2001/Data-Processing-](https://github.com/dicar-2001/Data-Processing-)
- **PySpark Documentation:** [spark.apache.org/docs/latest/api/python](https://spark.apache.org/docs/latest/api/python/)

## 🏆 Completion Checklist

- [ ] Virtual environment created and activated
- [ ] PySpark installed successfully
- [ ] LAB 1 executed without errors
- [ ] LAB 2 executed (Steps 1-13 successful)
- [ ] LAB 3 executed completely (all 13 steps)
- [ ] All labs pushed to GitHub

## 📝 License

Educational use for Data Processing course.

---

**Happy Learning! 🚀**

*Last Updated: November 22, 2025*

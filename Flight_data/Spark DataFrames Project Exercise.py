#!/usr/bin/env python
# coding: utf-8

# # Spark DataFrames Project Exercise 

# Let's get some quick practice with your new Spark DataFrame skills, you will be asked some basic questions about some stock market data, in this case Walmart Stock from the years 2012-2017. This exercise will just ask a bunch of questions, unlike the future machine learning exercises, which will be a little looser and be in the form of "Consulting Projects", but more on that later!
# 
# For now, just answer the questions and complete the tasks below.

# #### Use the walmart_stock.csv file to Answer and complete the  tasks below!

# #### Start a simple Spark Session

# In[2]:


import findspark


# In[3]:


findspark.init('/home/murali/spark-3.3.2-bin-hadoop3')


# In[6]:


import pyspark


# In[7]:


from pyspark.sql import SparkSession


# In[8]:


from pyspark.sql.types import StructField,StringType,IntegerType,StructType


# In[9]:


spark = SparkSession.builder.appName('test').getOrCreate()


# In[10]:


df = spark.read.csv('walmart_stock.csv', inferSchema=True,header=True)


# In[14]:


df.describe()


# In[15]:


df.printSchema()


# In[129]:


df.head(5)


# In[130]:


df.withColumnRenamed('date','NewDate').show()


# In[131]:


df.show()


# In[ ]:





# In[ ]:





# In[ ]:





# #### Load the Walmart Stock CSV File, have Spark infer the data types.

# In[16]:


df = spark.read.csv('walmart_stock.csv', inferSchema=True,header=True)


# #### What are the column names?

# In[18]:


df.columns 


# #### What does the Schema look like?

# In[20]:


df.printSchema()


# #### Print out the first 5 columns.

# In[76]:


df[][0:6].show()


# #### Use describe() to learn about the DataFrame.

# In[37]:


df.describe().show()


# ## Bonus Question!
# #### There are too many decimal places for mean and stddev in the describe() dataframe. Format the numbers to just show up to two decimal places. Pay careful attention to the datatypes that .describe() returns, we didn't cover how to do this exact formatting, but we covered something very similar. [Check this link for a hint](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.Column.cast)
# 
# If you get stuck on this, don't worry, just view the solutions.

# In[41]:


df.describe().printSchema()


# In[50]:


from pyspark.sql.functions import format_number
result = df.describe()
result.select(
                [
                result['summary'],
                format_number(result['open'].cast('float'),2).alias("Open"),
                format_number(result['High'].cast('float'),2).alias("High"),
                format_number(result['Low'].cast('float'),2).alias("Low"),
                format_number(result['Close'].cast('float'),2).alias("Close"),
                format_number(result['Volume'].cast('float'),2).alias("Volume")
                ]
).show()


# #### Create a new dataframe with a column called HV Ratio that is the ratio of the High Price versus volume of stock traded for a day.

# In[81]:





# In[91]:


from pyspark.sql.functions import (max,min,sum,avg,count)
max_val = df.agg(max(df['high'])).collect()[0][0]


# #### What day had the Peak High in Price?

# In[74]:


df.select('date').filter(df['high'] == max_val).collect()[0][0]


# #### What is the mean of the Close column?

# In[80]:


df.agg(avg(df['Close'])).show()


# #### What is the max and min of the Volume column?

# In[87]:


df.select([
    max('Volume'),
    min('Volume')
]).show()


# #### How many days was the Close lower than 60 dollars?

# In[97]:


df.filter(df['Close']<60).select(count('date')).collect()[0][0]


# #### What percentage of the time was the High greater than 80 dollars ?
# #### In other words, (Number of Days High>80)/(Total Days in the dataset)

# In[104]:


hi80 = df.filter(df['High']>80).select(count('date')).collect()[0][0]
days_cnt = df.select(count('date')).collect()[0][0]
(hi80/days_cnt)*100


# #### What is the Pearson correlation between High and Volume?
# #### [Hint](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrameStatFunctions.corr)

# In[110]:





# #### What is the max High per year?

# In[123]:


from pyspark.sql.functions import (year)
max_df = df.withColumn('Year',year(df['date'])).groupBy('Year').max()
max_df.select(['year','max(High)']).show()


# #### What is the average Close for each Calendar Month?
# #### In other words, across all the years, what is the average Close price for Jan,Feb, Mar, etc... Your result will have a value for each of these months. 

# In[121]:





# # Great Job!

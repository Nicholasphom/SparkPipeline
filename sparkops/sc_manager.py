from pyspark.sql import SparkSession, functions
from pyspark import SparkContext
import sparkops.spark_read_write as df_ops
import pyspark
from config import settings


class SparkContextManager():
    """A class for managing a SparkContext and creating a SparkSession.

    Attributes:
    ----------
    spark_context: SparkContext
        An object representing the SparkContext.

    Methods:
    -------
    __init__():
        Initializes the SparkContextManager object and creates a SparkSession.
    get_spark_context():
        Returns the SparkContext object.
    check_sc_connection():
        Checks if there is an active SparkContext and returns a boolean value.
    spark_connect():
        Connects to Spark if there is no active SparkContext.
    close_spark_session():
        Stops the SparkContext.

    """
    def __init__(self) -> None:
        self.spark_context = self.init_sc()
        self.sdf_readops = df_ops.SparkRead()

    def get_spark_context(self):
        return self.spark_context
    
    def check_sc_connection(self):
        try:
            
            if pyspark.SparkContext._active_spark_context is None:
            #self.spark_context.range(1).collect()
                print("SparkSession Not found, creating one")
                self.spark_context = self.init_sc()
                return False
     
            else:
                print("Spark Connection check - session found")
                self.spark_context = SparkSession.builder.getOrCreate()
                return True
        except Exception as e:
            print("Error creating SparkSession:", e)
            return False
    """
    def spark_connect(self):
        try:
            ## check for active sc connection first, if not then try to create one
            if self.check_sc_connection() is True:
                print("Spark Connection already exists..")
                return self.spark_context
            else:
                print("Connecting spark since a spark context is not found")
                  
        except Exception as e:
            print("Error in connecting to spark")
    """
    def close_spark_session(self):
        self.spark_context.stop()
    
            

    def init_sc(self):
        try:
            if pyspark.SparkContext._active_spark_context is None:
            #self.spark_context.range(1).collect()
                print("SparkSession Not found, creating one")
                if settings.SPARK_SETTING == 'local':
                    print("Setting up Local spark connection")
                else:
                    print(f"setting up configured spark connection {settings.SPARK_SETTING}")
                self.spark_context = SparkSession.builder.appName(settings.SPARK_APP_NAME).master(settings.SPARK_MASTER).getOrCreate()
            else:
                print("SparkSession Already found when trying to init_sc ")
                self.spark_context = SparkSession.builder.getOrCreate()
        except Exception as e:
            print("Error in Intitilizing spark context")
            

        
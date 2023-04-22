
from pyspark.sql import SparkSession

class SparkRead:
    """A class for reading data into a Spark dataframe.

    Methods:
    -------
    __init__():
        Initializes the SparkRead object.
    read_data(file_type, file_dir, **kwargs):
        Reads in data from a specified file location and returns a Spark dataframe.
    read_csv(file_dir, inferSchema=True, delimiter=",", header=True, **kwargs):
        Reads a CSV file into a Spark dataframe with the specified parameters.

    """
    def __init__(self):
       
        pass

    def read_data(self,file_type,file_dir, **kwargs):
        import sparkops.sc_manager as sc_manager
        # Base Read function
        print("Checking spark context")
        spark_manager = sc_manager.SparkContextManager()
        if spark_manager.check_sc_connection() is True:
            print("Spark session found and running read operation")

        if file_type == 'CSV':
            try:
                spark_df = spark_manager.spark_context.read.options(**kwargs).csv(file_dir)
                return spark_df
            except Exception as e:
                print("Error in reading csv data into spark ..",e)

    def read_csv(self,file_dir,inferSchema=True,delimiter = ",", header = True,**kwargs):
        
        try:
           
            print("Reading CSV file into spark dataframe ...")
            df = self.read_data(file_type="CSV",file_dir= file_dir,delimiter = ",", header = True,**kwargs)
            return df
        except Exception as e:
                print("Error in reading csv data into spark ..",e)


class SparkWrite():
    def __init__(self) -> None:
        pass
    def write_data(self,file_type,file_dir,mode = 'overwrite', **kwargs):
        import sparkops.sc_manager as sc_manager
        # Base Read function
        print("Checking spark context")
        spark_manager = sc_manager.SparkContextManager()
        if spark_manager.check_sc_connection() is True:
            print("Spark session found and running write operation")

        if file_type == 'CSV':
            try:
                print("Writing CSV")
                spark_df = spark_manager.spark_context.write.options(**kwargs).mode(mode).save(file_dir)
            except Exception as e:
                print("Error in writing csv data into spark ..",e)
    def write_csv(self,file_dir,mode = 'overwrite',delimiter = ",", header = True, **kwargs):
        pass
    
   
        

        
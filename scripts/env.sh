# source this: sets the GPU Python, Java 17 and PYTHONPATH for the pipeline
export JAVA_HOME="/c/Program Files/Eclipse Adoptium/jdk-17.0.20.101-hotspot"
export PATH="$JAVA_HOME/bin:/c/Users/Ankit/AppData/Local/Programs/Python/Python310:/c/Users/Ankit/AppData/Local/Programs/Python/Python310/Scripts:$PATH"
export PYSPARK_PYTHON="C:/Users/Ankit/AppData/Local/Programs/Python/Python310/python.exe"
export PYSPARK_DRIVER_PYTHON="$PYSPARK_PYTHON"
export PYTHONPATH=src
export PYTHONIOENCODING=utf-8
# Spark on Windows needs winutils.exe + hadoop.dll in tools/hadoop/bin
export HADOOP_HOME="D:/Big_Data/digital-archaeology/tools/hadoop"
export PATH="/d/Big_Data/digital-archaeology/tools/hadoop/bin:$PATH"

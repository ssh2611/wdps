PY_ENV=/var/scratch2/wdps1907/py_env/wdps1907_env
#source $PY_ENV/bin/activate

# Run Spark Job
PYSPARK_PYTHON=$PY_ENV/bin/python3 spark/spark-2.4.4-bin-hadoop2.7/bin/spark-submit \
--master yarn \
--conf spark.pyspark.virtualenv.enabled=true \
--conf spark.pyspark.virtualenv.type=native \
--conf spark.pyspark.virtualenv.bin.path=$PY_ENV/venv/bin/virtualenv \
--conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=$PY_ENV/venv/bin/python \
--conf spark.executor.extraLibraryPath=/cm/shared/package/gcc/6.4.0/lib64 \
--conf spark.driver.extraLibraryPath=/cm/shared/package/gcc/6.4.0/lib64 \
--conf spark.yarn.am.extraLibraryPath=/cm/shared/package/gcc/6.4.0/lib64 \
--num-executors 2 --executor-cores 2 --executor-memory 6GB \
--py-files code.zip \
--archives wdps1907_env.zip \
./starter-code.py "$1"

#deactivate

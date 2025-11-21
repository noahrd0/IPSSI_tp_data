PYTHON?=.venv/bin/python
STREAMLIT?=.venv/bin/streamlit
SPARK_SUBMIT?=.venv/bin/spark-submit
JAVA_HOME?=/home/noahrd0/jdks/jdk-17.0.11+9
HADOOP_HOME?=/home/noahrd0/hadoop-3.4.1
HDFS_BIN?=$(HADOOP_HOME)/bin/hdfs
HDFS_RAW?=file:///home/noahrd0/Documents/ipssi_tp_data/data/raw
HDFS_CURATED?=file:///home/noahrd0/Documents/ipssi_tp_data/data/curated

.PHONY: ingest etl dashboard all

ingest:
	$(PYTHON) -m ingestion.pipeline

etl:
	$(PYTHON) etl/transform.py

spark-etl:
	JAVA_HOME=$(JAVA_HOME) \
	PYSPARK_PYTHON=$(PYTHON) \
	$(PYTHON) etl/spark_transform.py \
		--raw-base-uri $(HDFS_RAW) \
		--curated-base-uri $(HDFS_CURATED) \
		--local-mirror /home/noahrd0/Documents/ipssi_tp_data/data/curated \
		--spark-conf spark.master=$${SPARK_MASTER:-local[*]}

spark-pipeline:
	@if [ ! -d "$(JAVA_HOME)" ]; then echo "JAVA_HOME invalide ($(JAVA_HOME))"; exit 1; fi
	@echo ">> Démarrage HDFS (namenode + datanode)"
	JAVA_HOME=$(JAVA_HOME) $(HDFS_BIN) --daemon start namenode
	JAVA_HOME=$(JAVA_HOME) $(HDFS_BIN) --daemon start datanode
	@echo ">> Ingestion batch locale"
	$(MAKE) ingest
	@echo ">> Synchronisation des dumps vers HDFS"
	JAVA_HOME=$(JAVA_HOME) $(HDFS_BIN) dfs -mkdir -p /datalake/raw
	JAVA_HOME=$(JAVA_HOME) $(HDFS_BIN) dfsadmin -safemode wait
	JAVA_HOME=$(JAVA_HOME) $(HDFS_BIN) dfs -put -f data/raw/* /datalake/raw
	@echo ">> ETL PySpark distribué"
	$(MAKE) spark-etl \
		HDFS_RAW=hdfs://localhost:9000/datalake/raw \
		HDFS_CURATED=hdfs://localhost:9000/datalake/curated \
		SPARK_MASTER=$${SPARK_MASTER:-local[*]}
	@echo ">> Lancement du dashboard Streamlit"
	$(MAKE) dashboard

all: ingest etl

dashboard:
	$(STREAMLIT) run insight/app.py

hãy code hoàn thiện lần lượt tệp trong dự án dưới đây của tôi sử dụng Hadoop, HBase, Kafka, Spark với dữ liệu đầu vào là NASA_access_log_Jul95.gz. 
data-processing/
├── hadoop/
│   ├── config.yml
│   ├── job.jar
│   └── log_processing/
│       ├── input/
│       │   └── logs.txt
│       ├── output/
│       └── src/
│           └── LogProcessor.java
├── hbase/
│   ├── config.yml
│   ├── create_table.sh
│   └── schema.txt
├── kafka/
│   ├── config.yml
│   ├── consumer/
│   │   ├── consumer.py
│   │   └── requirements.txt
│   ├── producer/
│   │   ├── producer.py
│   │   └── requirements.txt
│   └── topic/
│       ├── create_topic.sh
│       └── delete_topic.sh
└── spark/
    ├── config.yml
    ├── job.py
    └── log_processing/
        ├── input/
        │   └── logs.txt
        ├── output/
        └── src/
            └── log_processor.py. 
Hãy giúp tôi hoàn thiện dự án này nhé. Trước tiên hãy hoàn thiện đầy đủ thư mục hadoop nhé
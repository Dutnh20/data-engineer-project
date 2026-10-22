# Nguồn yêu cầu và tài liệu kỹ thuật

## Yêu cầu do người học cung cấp

- EDAI K11 - DE(1).xlsx, sheet edai-1 (50%), phần Implement Data Generator.
- coursework_proposal.md và các mô tả domain e-commerce trong cuộc trao đổi.

Các tỷ lệ 80%, 75%, 2%, 1,5% là kịch bản domain mẫu; 100GB là mức tối thiểu được
nêu rõ trong rubric offline. Mã nguồn có cấu hình riêng cho smoke và bài đủ volume.

## Tài liệu chính thức đã đối chiếu

- Apache Arrow, [Parquet write_table](https://arrow.apache.org/docs/python/generated/pyarrow.parquet.write_table.html).
- Apache Arrow, [Reading and writing Parquet](https://arrow.apache.org/docs/python/parquet.html).
- DuckDB, [Parquet overview](https://duckdb.org/docs/stable/data/parquet/overview).
- Confluent, [Python Kafka client](https://docs.confluent.io/kafka-clients/python/current/overview.html).
- AWS Boto3, [S3 upload_file](https://docs.aws.amazon.com/boto3/latest/reference/services/s3/client/upload_file.html).
- Docker, [Multi-stage builds](https://docs.docker.com/build/building/multi-stage/).

Requirements.lock khóa phiên bản đã cài và kiểm tra trong môi trường bàn giao;
không có nghĩa đó là phiên bản mới nhất. SDK có API thật nhưng kết nối Kafka và
MinIO cần được kiểm chứng trên stack của người học như README hướng dẫn.

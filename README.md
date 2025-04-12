# 🚀 Automated Data Pipeline with Apache Spark on AWS

## 📌 Overview

This project demonstrates a fully automated data pipeline using **Apache Spark on AWS EMR**, **AWS Lambda**, handling data from multiple heterogeneous sources — **Snowflake**, **Amazon S3**, and a **Web API**. The pipeline performs extraction, transformation, and joins across all sources to generate a final unified dataset, stored in Amazon S3 for downstream consumption. **AWS Lambda** is used for automation of tasks like invoking Spark jobs, managing triggers, and orchestrating the flow of the pipeline.

---

## 🛠️ Technologies Used

- Apache Spark (Scala)
- AWS EMR (Elastic MapReduce)
- Amazon S3
- Snowflake
- REST API
- AWS Lambda (for job automation)
- Eclipse (for packaging JARs)

---

## 📂 Data Sources

| Source        | Type                | Description                             |
|---------------|---------------------|-----------------------------------------|
| Snowflake     | Cloud Data Warehouse | Source table containing user-site data |
| S3            | Object Storage       | Raw transactional data                 |
| Web API       | Public API           | Nested JSON with customer data         |

---

## 📤 Output Locations (S3)

| Job Output                   | S3 Path                                           |
|-----------------------------|----------------------------------------------------|
| Snowflake (site counts)     | `s3://datastreamcorp/dest/sitecount`              |
| S3 Source (amount totals)   | `s3://datastreamcorp/dest/total_amount_data`      |
| Web API (flattened JSON)    | `s3://datastreamcorp/dest/customer_api`           |
| **Final Joined Data**       | `s3://datastreamcorp/dest/finalcustomer`          |
| JAR Backups                 | `s3://datastreamcorp/dir/`                        |

---

## 🧩 Spark Jobs

### 1️⃣ Snowflake Extraction Job
- **Extract**: From Snowflake table
- **Transform**: Count of `site` per `username`
- **Output**: Written to S3

### 2️⃣ S3 Data Processing Job
- **Extract**: Raw CSV/JSON data from S3
- **Transform**: Total `amount` per `username`
- **Output**: Written to S3

### 3️⃣ Web API Ingestion Job
- **Extract**: Data from REST API
- **Transform**: Flatten nested JSON fields
- **Output**: Written to S3

### 4️⃣ Master Join Job
- **Input**: Data from the above 3 jobs
- **Join On**: `username`
- **Output**: Final enriched dataset in S3

---

## ⚙️ Execution Flow

1. **Launch EMR Cluster**
   - With Apache Spark installed

2. **Setup AWS Lambda Functions**  
   - **Lambda Function**: Automates the execution of Spark jobs, ensuring that extraction and transformation tasks are triggered based on pre-defined schedules or events.

3. **SSH into EMR Cluster**

4. **Start Spark Shell**
   - Include Snowflake Connector for integration

5. **Run Extraction Jobs**  
   - Each job processes a source and writes to intermediate S3 location. Lambda triggers the jobs based on events or schedules.

6. **Run Master Job**  
   - Joins all datasets and writes final output to S3

7. **Generate JAR Files**  
   - Use Eclipse or similar IDE  
   - JARs for: `snow`, `s3`, `api`, and `master` jobs

8. **Upload JARs to S3**  
   - Path: `s3://datastreamcorp/ipldir/`

9. **Optional**: Delete intermediate data after validation

---

## 📦 JAR Structure in S3

### File Structure

```text
s3://datastreamcorp/
│
├── src/                           <- Raw input for S3 job
├── dest/
│   ├── sitecount/                <- Output of Snowflake extraction
│   ├── total_amount_data/       <- Output of S3 extraction
│   ├── customer_api/            <- Output of Web API extraction
│   ├── finalcustomer/           <- Final output (master job result)
│
└── dir/                       <- For backup JAR files
     ├── snow.jar
     ├── s3.jar
     ├── api.jar
     └── master.jar
```

## ✅ Final Notes

- All jobs are modular and reusable
- Pipeline is fully automated and cloud-native
- Ideal for production-scale data engineering pipelines

---

## 🧹 Post-Deployment

- Terminate EMR cluster to save costs
- Validate outputs in all S3 locations
- Maintain JARs for redeployment or CI/CD integration

---

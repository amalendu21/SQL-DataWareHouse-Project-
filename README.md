# 📊 SQL DataWareHouse Project

Welcome to the Data Warehouse and Analytics Project repository! 🚀 This project showcases a complete data warehousing and analytics workflow, covering everything from constructing the data warehouse to deriving meaningful insights. These were divided into multiple phases, from pulling the dataset from the customer environment to sending final reports to stakeholders.

### 💡 The Problem & Business Impact

* **Data Silos & Inconsistency:** The organization faced significant operational bottlenecks caused by disjointed data sources and inconsistent formatting across departments.
* **Manual Bottlenecks:** Data extraction from customer environments was heavily manual, leading to frequent delays in report generation.
* **Compromised Quality:** A lack of standardized preprocessing resulted in poor data quality and made reliable, long-term historical analysis impossible.
* **The Solution:** This project addresses these challenges by establishing an automated ELT pipeline that seamlessly ingests data and enforces rigorous validation across multi-layer processing (Bronze to Gold).
* **Business Value:** By converting raw data into optimized, secure analytical views, the pipeline eliminates data duplication, reduces manual efforts, and equips stakeholders with accurate, real-time insights for strategic decision-making.

### 📄 Project Documentation
For a deep dive into the complete architecture, data models, and detailed technical specifications, please refer to the full project documentation:
* 📁 **[Download Full Project PDF](https://drive.google.com/file/d/1OYMzYESXr2WU-CbLHGV8sPUV2l1gm8Ex/view?usp=sharing)**

---

### 📡 **Phase I: Data Ingestion & Collection**
1. Dedicated folder assigned to the Customers Dataset.
2. Data pulled from Customers SharePoint to our dedicated folder using a Python Script and Apache NiFi Processors.
3. Validating that the dataset pulled successfully.

### 🗄️ **Phase II: Bronze Layer (Raw Storage)**
1. Inspecting the data at the source.
2. Creating the Bronze Table with respective columns.
3. Executing the SQL Procedure to pull the data from the folder to the MySQL Schema as a Bronze Table.
4. Validating that the Bronze tables are filled with data.

### 🧹 **Phase III: Silver Layer (Data Cleansing)**
1. Analyzing the data at Bronze and identifying required cleanup.
2. Creating the Silver Table with analyzed and cleaned columns.
3. Executing the SQL Procedure to extract from Bronze and load into the Silver Table.

### 👑 **Phase IV: Gold Layer (Analytical Views)**
1. Analyzing and validating data from the Silver layer.
2. Converting data into VIEWs using Snowflake Data Modeling.
3. Making these VIEWs available for OLAP processing.

### 💼 **Phase V: Stakeholder Reporting & Governance**
1. Utilizing VIEWs in Power BI and applying Row-Level Security (RLS) for Data Governance.
2. Preparing and scheduling reports to be shared over email.

# 📊 SQL DataWareHouse Project

Welcome to the Data Warehouse and Analytics Project repository! 🚀 This project showcases a complete data warehousing and analytics workflow, covering everything from constructing the data warehouse to deriving meaningful insights. These were divided into multiple phases, from pulling the dataset from the customer environment to sending final reports to stakeholders.

---

### 📥 **Phase I (Collecting dataset from Customer Env to our Locals):**
1. Dedicated folder assigned to the Customers Dataset.
2. Data pulled from Customers SharePoint to our dedicated folder using a Python Script and Apache NiFi Processors.
3. Validating that the dataset pulled successfully.

### 🥉 **Phase II (Bronze Layer):**
1. Inspecting the data at the source.
2. Creating the Bronze Table with respective columns.
3. Executing the SQL Procedure to pull the data from the folder to the MySQL Schema as a Bronze Table.
4. Validating that the Bronze tables are filled with data.

### 🥈 **Phase III (Silver Layer):**
1. Analyzing the data at Bronze and identifying required cleanup.
2. Creating the Silver Table with analyzed and cleaned columns.
3. Executing the SQL Procedure to extract from Bronze and load into the Silver Table.

### 🥇 **Phase IV (Gold Layer):**
1. Analyzing and validating data from the Silver layer.
2. Converting data into VIEWs using Snowflake Data Modeling.
3. Making these VIEWs available for OLAP processing.

### 📈 **Phase V (Stakeholder Reporting):**
1. Utilizing VIEWs in Power BI and applying Row-Level Security (RLS) for Data Governance.
2. Preparing and scheduling reports to be shared over email.

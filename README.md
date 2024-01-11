# Data Pipeline and Analysis Documentation


=======
![image](https://github.com/Gospelmairo/ELT-Datapipeline-using-Airflow-and-DBT/assets/128805659/b5fc3215-19a5-4f6f-9331-f64a97771c80)
>>>>>>> af989902ecd58046117e626c4873d3aa46b552a9


## Project Overview
Welcome to the Vintage Stock Data project, where we seamlessly blend data orchestration with insightful analysis. This documentation provides a comprehensive guide to both the data pipeline orchestrated by Apache Airflow and the data modeling performed using dbt (data build tool).

Data Pipeline with Airflow
Airflow DAG: Load-Vintage-Data-From-Web-To-Postgres-GCS-To-BQ
This Airflow Directed Acyclic Graph (DAG) orchestrates the movement of vintage stock data from a web API to PostgreSQL, then to Google Cloud Storage (GCS), and finally to BigQuery.

Execution Steps
Web Data to PostgreSQL: Downloads vintage stock data using a custom operator (VintageToPostgresOperator) and loads it into PostgreSQL.
PostgreSQL to GCS: Uploads the data from PostgreSQL to GCS in JSON format using the PostgresToGCSOperator.
GCS to BigQuery: Transfers the data from GCS to BigQuery, creating a table named vintage_data_table.
DAG Schedule
The DAG runs daily at 21:00 UTC, ensuring the data is updated regularly.

Data Modeling with dbt
Folder Structure
markdown
Copy code
- models
  - staging
    - stg_vintage.sql
  - dimensions
    - dim_vintage_dates.sql
  - facts
    - fact_stock_prices.sql
  - marts
    - max_oclh_agg.sql
    - min_oclh_agg.sql
Staging: stg_vintage.sql
This model extracts raw vintage stock data from the vintage_data_table in the Alt_engin dataset.

Dimensions: dim_vintage_dates.sql
Transforms timestamps into meaningful dimensions like year, month, and day for better temporal understanding.

Facts: fact_stock_prices.sql
Defines the fact table fact_stock_prices with detailed vintage stock prices, serving as the core dataset.

Aggregates: max_oclh_agg.sql and min_oclh_agg.sql
Creates aggregated views showing daily maximum and minimum values for open, close, low, and high prices.

Conclusion
This integrated approach ensures a seamless flow from data extraction to insightful analysis. Whether you're managing the pipeline or delving into data modeling, this documentation equips you with the information needed for a successful exploration of vintage stock data.

Feel free to customize the models based on your requirements and execute the pipeline and models using Airflow and dbt, respectively.

Happy data engineering








Welcome to your new dbt project!

### Using the starter project

Try running the following commands:
- dbt run
- dbt test


### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [dbt community](https://getdbt.com/community) to learn from other analytics engineers
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices

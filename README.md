# End-to-End Customer Segmentation and Multi-Product Leads SQL Pipeline

## Project Overview
This repository contains an SQL-based pipeline for customer segmentation and generating multi-product offerings based on customers' assets under management (AUM) and payroll data. The purpose is to streamline the process of identifying high-value customers and providing them with personalized product recommendations.

The project includes:
- Customer segmentation based on their AUM.
- come analysis from payroll data.
- Integration of multiple product offerings for each customer.
- A final table that consolidates all relevant customer information for targeted marketing or sales campaigns.

## Problem Statement
The bank's marketing team needs an efficient way to target high-value customers with personalized financial products. However, the manual process of gathering customer income, assets under management, and product ownership information is time-consuming and prone to errors.

The specific challenges include:
1. **Customer Segmentation**: Determining customer segments based on their AUM to provide tailored offers.
1. **Multi-Product Offering**: Automatically generating a dynamic multi-product offer list for each customer.
1. **Data Consolidation**: Efficiently combining customer data from multiple sources (payroll, product ownership, etc.) into one coherent dataset.
1. **Automation of Lead Generation**: Automating the generation of leads for various sales programs with precise targeting.

## Data Sources
The pipeline pulls data from multiple sources:

1. Asset Under Management Table: Contains customer AUM data segmented by predefined tiers.
1. Customer Table: Contains basic customer information, including income.
1. Payroll Table: Contains payroll data that provides income details for each customer.
1. Product Tables: Contains product ownership information for various financial products.
1. Leads Table: The main table used to drive customer lead generation.

## Technologies Used
- SQL: The entire pipeline is built using SQL queries.
- Impala or Hive: The SQL queries are run on a big data platform like Impala or Hive to handle large datasets.
- Python: Optional for automation and report generation.
- Cloudera Machine Learning: (If applicable) for handling job scheduling and running SQL queries.

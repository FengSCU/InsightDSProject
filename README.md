# Insight Project: Neighborhood Finder

## Business Case
Home purchase is usually long and complicated process, even during research phase. People need to go back and forth to find information from multiple places. It's inefficient. My solution to that is to create a dashboard that combining multiple resources toghter that users can use it to identify favorable neighborhoods. 

To illustrate, users can use the dashboard to check:

1.What's the most pupular areas people buy homes in their income level? 

a)Income could be a indicator as affortablity, since income is the major factor to determine the loan people can get. 

b)Define the area/neighborhood: I use 'census tract' as the baseline for neighbor/area. **Census Tracts** are small, relatively permanent statistical subdivisions of a county that defined by Census Bureau. 

2.How many crime/incident happened in the areas? Does the crime increase or decrease over years?

3.How's the school nearby? 

Identifying the neighbors will help home buyers more focus that shorten the purchase process and make fewer trials.

## Datasets
### Raw Datasets
I use following datasets for the of project:

1.Mortgage datasets from Consumer Financial Protection Bureau

This dataset contains millions of records of home loan application in the US. It contains loan applicant demographic information (gender, race, income), property information associated with the loan, and loan details(loan amount, loan to value ratio, interest_rate, etc).

2.Crime datasets, such as San Francisco crime datasets from SF open data

This dataset includes daily incident records published by City police. It tells about the incident datetime, incident category, and incident location.

3.School datasets from greatschool: 

This dataset contains school records, such as enrollment, rating, and names.

### Designed Datasets
The following tables will be produced for the dashboard:
1. School: tract, school names, enrollment, grade, rating, lat,long
2. Tract: tract, year, population, ave_home_age, num_of_owners_occupied, num_of_1to4_units
3. Crime: tract, year, total_count, crime_rate(total_count/pop)
4. Mortage: tract,year,income_busket, total_count, rank

## Setup & ETL Pipeline
The datasets are extracted from web and store to S3, precessed by spark, and the designed datasets will be stored to postgreSQL, and be visulized by Tableau.

This project is city-based project, I will use San Francisco city as an example to design the ETL pipeline.  


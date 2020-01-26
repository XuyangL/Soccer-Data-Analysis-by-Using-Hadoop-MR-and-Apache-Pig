# Final Project

**Soccer Data Analysis by Using Hadoop MR and Apache Pig**    
Final Project for Engineering of Big Data Systems

## Author

Xuyang Li 001409590

## Project Overview
The name of this class is Engineering of Big Data System and for the final project, we have to implement a system which can analyze some files and get the results. The professor has emailed us several links about the datasets we can use. I carefully checked each one and get some basic ideas about the whole project. First, the files don’t need to be too large. Although there are several datasets which are quite large, (for example, the customer review dataset of Amazon, which is typically over 3GB for each file), we need to find our suitable datasets. Second, some of the datasets cannot be analyzed by using MapReduce. For example, there are a lot of datasets which contain image files or meaningful content. They are good datasets for machine learning but not for MapReduce analysis. MapReduce is effective when analyzing number-intensive files. 

As a super fan of soccer, I realized that it is very good to find a dataset which has the data of soccer matches, since in a soccer match report there are a lot of values, such as goals, number of shots, number of fouls, number of yellow cards, etc. The numbers in the match report is straightforward. Plus, the number of soccer matches are large but not too large: there are usually 20 teams in a league and 380 matches a year. We can choose 5 leagues, analyze the recent ten years data of each league. What’s more, as soccer fan, I know the rules of soccer and know how to analyze the data to find some interesting results.

This project contains four interesting analysis on soccer data. For more information of the data set, please refer to the final report.docx. Each analysis is implemented in Hadoop MR and Apache Pig, which gives us a good comparision on these data analysis methods.





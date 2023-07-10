# QA Activity Summary

This repository contains the SQL code used to create a table summarizing the activity generated by a QA team. The goal of this table is to generate a subsequent dashboard for monitoring and analyzing the QA team's activity.

## 📊 Data Sources

The table is created by linking data from two sources:

1. **Jira**: Jira is a project management tool used for tracking and managing software development tasks. The tickets and tasks performed by the QA team are loaded into Jira, and their data is extracted for analysis.

2. **Testrail**: Testrail is a software specifically designed for developing and monitoring quality tests on software. The QA team uses Testrail to execute and manage their test cases. The data from Testrail is also extracted and linked with the Jira data to create a comprehensive QA activity summary.

## 🗄️ Table Creation

The SQL code provided in this repository creates a table that combines relevant information from Jira and Testrail to summarize the QA team's activity. The table includes fields such as ticket/task ID, summary, assignee, status, test case ID, test case title, test result, and any additional relevant information.

To use this SQL code, follow these steps:

1. Set up the necessary database connection and ensure you have the required privileges.

2. Execute the SQL code provided in the `create_table.sql` file.

## 📧 Contact

For any questions or inquiries, feel free to contact me.

Enjoy!
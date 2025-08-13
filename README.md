# 📦 S3 Space Consumption Analysis & Reporting

## 📋 Overview
This project analyzes AWS S3 bucket storage usage, calculates folder sizes, and generates an interactive HTML report.  
The report is optionally appended to existing historical reports and can be emailed automatically to recipients.

---

## 🚀 Features
- **Bucket Summary**: Total size (GB) and total file count.
- **Recursive Folder Analysis**: Calculates size for each folder/subfolder.
- **Interactive HTML Report**: Expand/collapse folder structure with size details.
- **Historical Reports**: Append new report data to an existing HTML file in S3.
- **Email Notifications**: Sends HTML report as an email attachment.
- **Databricks Compatible**: Runs inside a Databricks notebook or locally.

---

## 🛠️ Tech Stack
- **Language**: Python 3
- **Environment**: Databricks Notebook (or local Python)
- **Cloud Services**:
  - AWS S3 (Storage & HTML hosting)
  - AWS Boto3 SDK (S3 access)
  - SMTP Server (Email sending)
- **Libraries**:
  - boto3
  - smtplib
  - email.mime
  - datetime
  - os

---

## 📂 Project Structure

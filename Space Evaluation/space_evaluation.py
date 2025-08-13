# Databricks notebook source
import boto3
from datetime import datetime

# Function to calculate the total size (in GB) and number of files in an S3 bucket
def get_bucket_size(bucket_name):
    s3 = boto3.client('s3')
    total_size = 0
    total_files = 0

    # Paginate through all bucket objects to handle large buckets
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket_name):
        for obj in page.get('Contents', []):
            total_size += obj['Size']
            total_files += 1

    # Convert size to GB
    total_size_gb = total_size / (1024 ** 3)
    return total_size_gb, total_files



# Recursive function to calculate S3 folder sizes in a nested dictionary format
def calculate_s3_folder_sizes(bucket_name, prefix='', depth=0):
    """
    Creates a nested dictionary representing folder structure and sizes.
    Skips certain folders like checkpoints, dateref, etc.
    """
    s3 = boto3.client('s3')
    paginator = s3.get_paginator('list_objects_v2')

    folder_structure = {"name": prefix.rstrip('/'), "size_gb": 0, "subfolders": {}}
    
    # Calculate size of all files in the current prefix
    total_size = 0
    # List files and common prefixes (folders) for current level
    page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix, Delimiter='/')
    
    for page in page_iterator:
        # Sum sizes of files directly under current prefix
        for obj in page.get('Contents', []):
            total_size += obj['Size']
        
        # Iterate over subfolders
        for common_prefix in page.get('CommonPrefixes', []):
            folder_prefix = common_prefix['Prefix']

            # Skip subfolders inside 'folder_name'
            if folder_prefix == 'folder_name':
                continue
        
            # Calculate size for the current subfolder
            folder_size = 0
            folder_paginator = paginator.paginate(Bucket=bucket_name, Prefix=folder_prefix)
            for folder_page in folder_paginator:
                for obj in folder_page.get('Contents', []):
                    folder_size += obj['Size']
            
            subfolder = {
                "name": folder_prefix.rstrip('/'),
                "size_gb": folder_size / (1024 ** 3),  # Convert size to GB
                "subfolders": {}
            }
            folder_structure['subfolders'][folder_prefix] = subfolder
            
            # Print the current folder size with indentation based on depth
            print(f"{'   ' * depth}{folder_prefix} - {subfolder['size_gb']:.2f} GB")
            
            # Recursively calculate sizes for subfolders
            if 'date' not in folder_prefix: # suppose we want to calculate the sizes till date partition
                subfolder_structure = calculate_s3_folder_sizes(bucket_name, folder_prefix, depth + 1)
                subfolder['subfolders'] = subfolder_structure['subfolders']
    
    folder_structure['size_gb'] = total_size / (1024 ** 3)  # Update size for the current folder
    return folder_structure



# -------------------------------------------------------------------
# MAIN EXECUTION
# -------------------------------------------------------------------

# Main Execution: Analyze bucket size and structure
bucket_name = 's3_bucket_name'

# Get current date
current_date = datetime.now().strftime("%d-%m-%Y")

# Get bucket size and create top-level date folder
size_gb, total_files = get_bucket_size(bucket_name)
date_structure = {
    "name": f"Date : {current_date}, Total Storage Size ",
    "size_gb": size_gb,
    "subfolders": {
        bucket_name: {
            "name": f"Bucket Name : {bucket_name} , Total Files : {total_files} , Total Bucket Size",
            "size_gb": size_gb,
            "subfolders": {}
        }
    }
}

# Print the top-level structure
print(f"Date - {current_date} - {size_gb:.2f} GB")
print(f"   Name: {bucket_name}, Size: {size_gb:.2f} GB")

# Calculate and print sizes of folders inside the bucket
folder_structure = calculate_s3_folder_sizes(bucket_name)
date_structure["subfolders"][bucket_name]["subfolders"] = folder_structure['subfolders']



# -------------------------------------------------------------------
# HTML Generation Functions
# -------------------------------------------------------------------

# Recursively build collapsible HTML elements for folders
def generate_html_for_folder(folder):
    """Recursively generates HTML for a folder and its subfolders."""
    # Create a collapsible details element
    html = f"<details><summary>{folder['name']} - {folder['size_gb']:.2f} GB</summary>"
    
    # Recursively add subfolders
    for subfolder in folder['subfolders'].values():
        html += generate_html_for_folder(subfolder)
    
    # Close the details element
    html += "</details>"
    
    return html



# Generate full HTML page with styles and collapsible folder tree
def create_html_output(folder_structure):
    """Generates the full HTML output."""
    html_output = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>S3 Folder Sizes</title>
        <style>
            body {{
                font-family: 'Arial', sans-serif;
                background-color: #f7fafc;
                color: #333;
                margin: 0;
                padding: 20px;
            }}
            h1 {{
                text-align: center;
                font-size: 2.5rem;
                color: #2c3e50;
                margin-bottom: 30px;
            }}
            summary {{
                cursor: pointer;
                font-size: 1.1rem;
                padding: 10px;
                background-color: #d6d9dc;
                color: #333;
                border-radius: 8px;
                transition: background-color 0.3s ease;
            }}
            summary:hover {{
                background-color: #e1e3e5;
            }}
            details {{
                margin-left: 20px;
                margin-top: 10px;
            }}
            details[open] summary {{
                background-color: #f2f3f5;
            }}
            p {{
                font-size: 1rem;
                color: #7f8c8d;
                margin: 8px 0;
            }}
            details {{
                box-shadow: 0px 2px 5px rgba(0, 0, 0, 0.1);
                padding: 10px;
                background-color: #ecf0f1;
                border-radius: 6px;
            }}
            .folder-size {{
                font-weight: bold;
                color: #333;
            }}
            .no-size {{
                display: none;
            }}
        </style>
    </head>
    <body>
        
        {generate_html_for_folder(date_structure)}
    </body>
    </html>
    """
    return html_output


# Assuming `folder_structure` is generated as before
html_output = create_html_output(date_structure)


# Save HTML to a local file
def save_html_to_file(html_content, file_path):
    """Save HTML content to a file."""
    with open(file_path, 'w') as file:
        file.write(html_content)

# COMMAND ----------

html_file_path = 's3_output.html'

save_html_to_file(html_output, html_file_path)

print(f"HTML file saved to {html_file_path}. Open this file in a web browser to view the folder sizes.")

# COMMAND ----------

import os
local_file_path = 's3_output.html'
with open(local_file_path, "r") as f:
    data = "".join([l for l in f])
if "DATABRICKS_RUNTIME_VERSION" in os.environ:
    from databricks.sdk.runtime import displayHTML
    displayHTML(data)
else:
    from IPython import display, HTML
    display(HTML(data))

# -------------------------------------------------------------------
# S3 HTML Upload/Download & Append Functions
# -------------------------------------------------------------------

# Download an HTML file from S3
import boto3

def download_html_from_s3(bucket_name, s3_path, local_file_path):
    """Downloads an HTML file from S3."""
    s3 = boto3.client('s3')
    
    s3.download_file(bucket_name, s3_path, local_file_path)
    print(f"Downloaded HTML file from s3://{bucket_name}/{s3_path}")


def append_html_to_existing_file(local_file_path, new_html_content):
    """Appends new HTML content to the existing file."""
    with open(local_file_path, 'r+') as file:
        content = file.read()
        insert_position = content.rfind('</body>')
        
        if insert_position != -1:
            updated_content = content[:insert_position] + new_html_content + content[insert_position:]
            file.seek(0)
            file.write(updated_content)
            file.truncate()
            return updated_content
        else:
            print("Error: Couldn't find </body> tag in the file.")
            return content
    

def upload_html_to_s3(bucket_name, s3_path, html_file_path):
    """Uploads the updated HTML file to S3."""
    s3 = boto3.client('s3')

    # Read the updated HTML file
    with open(html_file_path, 'r') as file:
        html_content = file.read()

    # Upload the updated HTML file back to S3
    s3.put_object(
        Bucket=bucket_name,
        Key=s3_path,
        Body=html_content,
        ContentType='text/html'  # Specify the content type for HTML
    )
    print(f"Updated HTML file uploaded to s3://{bucket_name}/{s3_path}")

bucket_name =  's3_bucket_name'
s3_path = 'space_consumption/output/html/s3_output.html'  # Specify the path in S3 where you want to upload the HTML file
local_html_file_path = 's3_output.html'  # Local path to your HTML file

# Step 1: Download the existing HTML file from S3
download_html_from_s3(bucket_name, s3_path, local_html_file_path)

# Step 2: Append new HTML content (from your generate_html_output function)
new_html_content = create_html_output(date_structure)  # Generate the new HTML content
updated_html_content = append_html_to_existing_file(local_html_file_path, new_html_content)

# # Step 3: Upload the updated HTML file back to S3
upload_html_to_s3(bucket_name, s3_path, local_html_file_path)

# COMMAND ----------

displayHTML(updated_html_content)

# -------------------------------------------------------------------
# Email Notification Functions
# -------------------------------------------------------------------

smtp_host_address = "smtp_host_address_name"
from_email ='from_email@gmail.com'
to_email = 'to_email@gmail.com'


# COMMAND ----------

def read_html_from_s3(bucket_name, s3_key):
    s3 = boto3.client('s3')
    
    # Retrieve the HTML file from S3
    response = s3.get_object(Bucket=bucket_name, Key=s3_key)
    
    # Read the file content
    html_content = response['Body'].read().decode('utf-8')
     
    # Optionally, return the HTML content to use it elsewhere in your script
    return html_content

html_content = read_html_from_s3(bucket_name,s3_path)
displayHTML(html_content)
send_notification_email('S3_SIZE',' ',bucket_name,s3_path,smtp_host_address, from_email, to_email)

# COMMAND ----------

import os
import boto3
import smtplib
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

def send_email(
    smtp_host_address,
    port=None,
    from_email="",
    to_email="",
    subject="",
    body="",
    attachment_path_list=[],
    body_type="html",
):

    msg = MIMEMultipart()
    msg["From"] = from_email
    msg["To"] = to_email
    msg["Subject"] = subject

    # Attach HTML report as body or as attachment
    for attachment_path in attachment_path_list:
        with open(attachment_path, "rb") as attachment:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(attachment.read())
            encoders.encode_base64(part)
            part.add_header(
                "Content-Disposition",
                f'attachment; filename="{attachment_path.split("/")[-1]}"',
            )
            msg.attach(part)

    # Attach body
    msg.attach(MIMEText(body, body_type))

    # Connect to SMTP server and send email
    if port:
        with smtplib.SMTP(smtp_host_address, port) as server:
            server.sendmail(from_email, to_email, msg.as_string())
    else:
        with smtplib.SMTP(smtp_host_address) as server:
            server.sendmail(from_email, to_email, msg.as_string())


def download_html_from_s3(bucket_name, s3_key, local_path):
    """Download an HTML file from S3 to a local path"""
    s3 = boto3.client('s3')
    
    # Download the HTML file from S3
    s3.download_file(Bucket=bucket_name, Key=s3_key, Filename=local_path)

    # Return local path for the downloaded file
    return local_path


def send_notification_email(segment_string, excel_name, s3_bucket_name, s3_html_key, smtp_host_address, from_email, to_email):

    import datetime
    today_str = datetime.date.today().strftime("%Y-%m-%d")

    msg_title = f" {today_str}: {segment_string}Space Consumption Analysis Report"

    body = f"""
    <html>
    <body>
    <p>Hi Team,</p>

    <p>
    Please find attached the Space Consumption Analysis Report for the that Environment related to the segment: <strong>{segment_string}</strong>.</p>
    <p>The report provides an overview of the current storage utilization as of {today_str}.</p>

    <p>Thank you for your attention.</p>
    

    </body>
    </html>
    """

    base_path = os.getcwd() + "/"
    
    # Download the HTML file from S3
    html_filename = "s3_output.html"
    html_local_path = download_html_from_s3(s3_bucket_name, s3_html_key, base_path + html_filename)

    # List files in base path
    files_list = os.listdir(base_path)
    attachement_path_list = []

    for file_name in files_list:
        if excel_name in file_name or file_name == html_filename:
            attachement_path_list.append(base_path + file_name)
    
    # Send email with attachments
    send_email(
        smtp_host_address=smtp_host_address,
        from_email=from_email,
        to_email=to_email,
        subject=msg_title,
        body=body,
        attachment_path_list=attachement_path_list,
    )


#/**********************************************************************************************************/




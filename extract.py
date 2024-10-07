import csv
from faker import Faker
import random
import string
from google.cloud import storage

# Specify number of employees to generate
num_employees = 100

# Create Faker instance
fake = Faker()

# Define the character set for the password
password_characters = string.ascii_letters + string.digits + 'm'

# Generate employee data and save it to a CSV file
with open('employee_data.csv', mode='w', newline='') as file:
    fieldnames = ['first_name', 'last_name', 'job_title', 'department', 'email', 'address', 'phone_number', 'salary', 'password']
    writer = csv.DictWriter(file, fieldnames=fieldnames)

    writer.writeheader()
    for _ in range(num_employees):
        writer.writerow({
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "job_title": fake.job(),
            "department": fake.job(),  # Generate department-like data using the job() method
            "email": fake.email(),
            "address": fake.city(),
            "phone_number": fake.phone_number(),
            "salary": fake.random_number(digits=5),  # Generate a random 5-digit salary
            "password": ''.join(random.choice(password_characters) for _ in range(8))  # Generate an 8-character password with 'm'
        })

def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the Google Cloud Storage bucket."""
    
    # Initialize a storage client with default credentials (your personal account)
    storage_client = storage.Client()

    # Get the bucket from Google Cloud Storage
    bucket = storage_client.get_bucket(bucket_name)

    # Create a blob (object) in the bucket
    blob = bucket.blob(destination_blob_name)

    # Upload the file to the blob
    blob.upload_from_filename(source_file_name)

    print(f"File {source_file_name} uploaded to {destination_blob_name}.")

# Replace these with your values
bucket_name = "bucket-employee-data-1"
source_file_name = "employee_data.csv"
destination_blob_name = "employee_data.csv"

# Call the function to upload
upload_to_gcs(bucket_name, source_file_name, destination_blob_name)

# Bigquery-Containers-on-Google-Cloud
- This mini project shows how to build docker images, push them to artifact registry and access google cloud APIs
while running their containers, all on Google Cloud.
- In this project I'm building a docker image, reading data from google cloud storage and inserting them to a bigquery table.
- Bigquery dataset and table will be created automatically if they don't exist.
- Google cloud logging is setup in the script and all log messages will be written to cloud logging.

## Steps to run
- Activate google cloud shell.
- Clone this repository.
```shell
git clone https://github.com/Subrahmanyajoshi/Docker-Containers-on-Google-Cloud.git
```
- Navigate to dockerize folder.
```shell
cd dockerize
```
- In this project, I have used Iris dataset. Any structured dataset can be used. It's schema needs to be updated inside 
the bigquery_pusher.py script.
- Create a google cloud service account, and it's associated key. 'Account-Name' can be any desired name.
```shell
gcloud iam service-accounts create <Account-Name> --display-name "api account"
gcloud iam service-accounts keys create ./key.json --iam-account <Account-Name>@$<Project-Id>.iam.gserviceaccount.com
```
- Above command will create a key.json containing google cloud account credentials. It needs to be stored in 'dockerize'
folder to be available inside the docker container.
- Give storage admin and bigquery admin roles to newly created service account.
```shell
gcloud projects add-iam-policy-binding <Project-Id> --member=serviceAccount:<Account-Name> --role=roles/bigquery.admin
gcloud projects add-iam-policy-binding <Project-Id> --member=serviceAccount:<Account-Name> --role=roles/storage.admin
```
- Navigate to project root.
```shell
cd ..
```
- Create an artifact repository if it doesn't exist. This needs to be run only once.
```shell
gcloud artifacts repositories create docker-repo --repository-format=docker --location=us-central1 --description="Docker repository"
```
- Build the docker image and push it to the created artifact repository.
```shell
gcloud builds submit --tag us-central1-docker.pkg.dev/<Project-Id>/docker-repo/bigquery-image:0.1
```
- Run container. --gcs-path must be a full path to a csv file in GCS (should start with gs://).
```shell
docker run -it us-central1-docker.pkg.dev/<Project-Id>/docker-repo/bigquery-image:0.1 --gcs-path=<Csv-File-path-in-GCS>
```
- Logs can be seen in Google Cloud logs explorer.
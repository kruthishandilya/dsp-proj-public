

# Data science in Production

# E- Commerce Fraud Detection 

The system includes:

 1.A FastAPI - API serving predictions with at least /health and /predict endpoints
 
 2.Streamlit webapp - prediction page that calls the API
 
 3.PostgreSQL database - predictions table, API reads/writes via SQLAlchemy
 
 4.Docker Compose - all 3 services running together with docker compose up
 
 5.Data splitting script - takes a dataset and splits it into N files in raw_data
 
 6.Data error injection script - takes a clean dataset and injects errors with configurable probability ( 7 error types)
 
 All services run together using Docker Compose.

## Tech Stack Used 

  FastAPI  
  Streamlit  
  PostgreSQL  
  SQLAlchemy  
  Docker & Docker Compose  

## Features

### API Endpoints

 1.`GET /health`  
  Health check endpoint for container readiness.

 2.`GET /predictions`  
  Retrieves previously stored predictions.

 3.`POST /predict` (Single Prediction)  
  Accepts feature input for a single sample.  
  Returns prediction and stores it in the database.

 4.`POST /predict` (Batch Prediction)  
  Accepts multiple records in a single request.  
  Returns predictions for all records and stores them in the database

## Streamlit Web App

The web application:

  Checks API health
  Sends prediction requests to the API
  Displays prediction results
  Displays past predictions


All interactions go through the API.

## Database

PostgreSQL stores:

- Input features
  
- Prediction results
  
- Timestamps

The API interacts with the database using SQLAlchemy ORM.


## Utility Scripts

### 1.Data Splitting
`scripts/split_data.py`

Splits a dataset into N smaller files inside a target directory.

### 2.Error Injection
`scripts/inject_errors.py`

Injects configurable data quality issues (minimum 7 error types including nulls, invalid ranges, datatype issues, duplicates, and schema errors).

## For Running the Project

### 1.Build and Start Services

     - docker compose up --build

### 2.Access Services

- Streamlit: http://localhost:8501
  
- API: http://localhost:8000
    
- PostgreSQL: runs internally within Docker network  

## Note :

 1.All services are containerized.
 
 2.Services communicate via Docker network.
 
 3.No direct model access from the web application.




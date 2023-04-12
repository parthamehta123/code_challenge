***Code Challenge Template***

***Project Setup***

- Python Version: python>=3.9
- Create and activate a virtual environment - using Makefile or manual 
  - make venv or manual: 
  - python3 -m venv <env>
  - source <env>/bin/activate 
- Set Django settings 
  - export DJANGO_SETTINGS_MODULE="code_challenge.settings"
- Apply Database Migrations 
  - python3 manage.py migrate

***Testcases***

- Using Nosetests to run tests for the project:
  - nosetests .
    
***Data Ingestion***

- Ingesting crop and weather data using following commands 
  - python3.9 manage.py import_weather_data 
  - python3.9 manage.py import_crop_data
      
***Data Analysis***

- Weather Stats are computed for pair of station_id and year possible using following command:
  - python manage.py weather_analysis
        
***REST APIs***
        
- Django REST Framework was used to develop the following 3 REST API GET endpoints with default pagination
  50 records and filter arguments per assignment:
  - /api/weather 
  - /api/weather/stats 
  
  Examples below and refer api postman screenshots in Answers folder for the following GET api responses:

  http://localhost:8000/api/weather
  http://localhost:8000/api/weather?page=172996
  http://localhost:8000/api/weather/stats
  http://localhost:8000/api/weather/stats?page=10
  http://localhost:8000/api/weather/stats?page=1&year=1989&station_id=USC00255080

***Download REST API Export I have done from the below link***


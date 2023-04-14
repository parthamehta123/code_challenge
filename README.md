***Code Challenge Template***

***Project Setup***

- Python Version: python>=3.9
- Create and activate a virtual environment - using Makefile or manual 
  - make venv or manual
  - python3 -m venv venv
  - source venv/bin/activate
- Set Django settings 
  - export DJANGO_SETTINGS_MODULE="code_challenge.settings"
- Apply Database Migrations 
  - python3 manage.py migrate please refer migrations.png for reference in Answers folder

***Testcases***

- Using Nosetests to run tests for the project:
  - nosetests . Please refer nosetests.png in answers folder for reference
    
***Data Ingestion***

- Ingesting crop and weather data using following commands 
  - python3.9 manage.py import_weather_data 
  - python3.9 manage.py import_crop_data
  - please see the screenshot - Dataingestion for crop and weatherdata.png for reference in Answers folder
      
***Data Analysis***

- Weather Stats are computed for pair of station_id and year possible using following command:
  - python manage.py weather_analysis
  - please see the screenshot - weather_analysis.png for reference in Answers folder
        
***REST APIs***
        
- Flask REST Framework was used to develop the following 2 REST API GET endpoints with default pagination
  10 records and filter arguments per assignment:
  - /api/weather 
  - /api/weather/stats 
  
  Examples below and refer api postman screenshots in Answers folder for the following GET api responses:

  http://localhost:8001/api/weather
  http://localhost:8001/api/weather?page=172996
  http://localhost:8001/api/weather/stats
  http://localhost:8001/api/weather/stats?page=10
  http://localhost:8001/api/weather/stats?page=1&year=1989&station_id=USC00255080

***Download REST API Export I have done from the below link***

https://github.com/parthamehta123/code_challenge/blob/master/ColaberryAssessment.postman_collection.json

***Including a Swagger/OpenAPI endpoint that provides automatic documentation of your API.***

To include a Swagger/OpenAPI endpoint in a Django project, we have to use the drf-yasg library which provides an easy-to-use and customizable interface for generating API documentation. Here are the steps:

1. Install the drf-yasg library using pip:

***pip install drf-yasg***

2. Add the drf_yasg app to the list of installed apps in your Django project's settings.py file:

***INSTALLED_APPS = [
    # other apps
    'drf_yasg',
]***

<img width="1792" alt="image" src="https://user-images.githubusercontent.com/25328046/231945306-c00c3df7-fd99-48df-aa6c-93d7576cceb7.png">

3. Adding the drf_yasg.views and rest_framework URLs to the project's urls.py file: (Here we add the code for the Swagger endpoint to "code_challenge/code_challenge/urls.py". This file is the main URL configuration for our project, so it's a good place to add any additional URL patterns.

<img width="1792" alt="image" src="https://user-images.githubusercontent.com/25328046/231945240-1f6071f2-e702-41b2-b4d0-9359fcd6cf96.png">

4. Type 'python manage.py runserver' on the terminal like shown in the screenshot below:

<img width="1792" alt="image" src="https://user-images.githubusercontent.com/25328046/231963659-b3fd6d86-a14e-43bd-9e9b-40669844b7f7.png">

5. Then go to the link which shows up in the command line and test out if the Swagger requirement of auto-documentation is working or not

<img width="1792" alt="image" src="https://user-images.githubusercontent.com/25328046/231964094-a3128a95-f081-4514-92ab-3240a7b00a8a.png">

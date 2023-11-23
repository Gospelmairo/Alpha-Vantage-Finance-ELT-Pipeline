## Setup (official)

### Requirements
1. Upgrade docker-compose version:2.x.x+
2. Allocate memory to docker between 4gb-8gb
3. Python: version 3.8+ 


### Set Airflow 

1.  On Linux, the quick-start needs to know your host user-id and needs to have group id set to 0. 
    Otherwise the files created in `dags`, `logs` and `plugins` will be created with root user. 

2.  Create directory using: 
     
    ```bash
    mkdir -p ./dags ./logs ./plugins
    echo -e "AIRFLOW_UID=$(id -u)" > .env
    ```

    For Windows same as above. 

    Create `.env` file with the content below as:

    ```
    AIRFLOW_UID=50000
    ```

3. Download or import the docker setup file from airflow's website
    
   ```bash
   curl -LfO 'https://airflow.apache.org/docs/apache-airflow/stable/docker-compose.yaml'
   ```
4. Create "Dockerfile" use to build airflow container image.
5. Build image: docker-compose build
6. Initialize airflow db; docker-compose up airflow-init
7. Initialize all the other services: docker-compose up
8. Connect external postgres container to the airflow container by assigning the airflow_defult netwrk to the pgres container: see the yaml file
9. Check for postgres db access from the airflow container.


   

@ECHO OFF
ECHO Checking existing Docker images ...

:: Set image names
SET AIRFLOW_IMAGE=banking-airflow:custom
SET DEBEZIUM_INIT=debezium-connector:custom
SET CDC_CONSUMER=cdc-consumer:custom

:: Check if AIRFLOW_IMAGE already exists
SET BUILD_AIRFLOW_IMAGE=true
FOR /F "tokens=*" %%i IN ('docker images -q %AIRFLOW_IMAGE%') DO (
    IF NOT "%%i"=="" (
        ECHO [SKIP] %AIRFLOW_IMAGE% already exists. Skipping build.
        SET BUILD_AIRFLOW_IMAGE=false
    )
)

:: Check if DEBEZIUM_INIT already exists
SET BUILD_DEBEZIUM_INIT=true
FOR /F "tokens=*" %%i IN ('docker images -q %DEBEZIUM_INIT%') DO (
    IF NOT "%%i"=="" (
        ECHO [SKIP] %DEBEZIUM_INIT% already exists. Skipping build.
        SET BUILD_DEBEZIUM_INIT=false
    )
)

:: Check if CDC_COMSUMER image already exists
SET BUILD_CONSUMER=true
FOR /F "tokens=*" %%i IN ('docker images -q %CDC_CONSUMER%') DO (
    IF NOT "%%i"=="" (
        ECHO [SKIP] %CDC_CONSUMER% already exists. Skipping build.
        SET BUILD_CONSUMER=false
    )
)

IF "%BUILD_AIRFLOW_IMAGE%"=="true" (
    ECHO [BUILD] Building %AIRFLOW_IMAGE% image ...
    docker build -t %AIRFLOW_IMAGE% -f dockerfile-airflow.dockerfile .
    ECHO [INFO] %AIRFLOW_IMAGE% image build completed.
)

IF "%BUILD_DEBEZIUM_INIT%"=="true" (
    ECHO [BUILD] Building %DEBEZIUM_INIT% image ...
    docker build -t %DEBEZIUM_INIT% -f dockerfile-debezium-init.dockerfile .
    ECHO [INFO] %DEBEZIUM_INIT% image build completed.
)

IF "%BUILD_CONSUMER%"=="true" (
    ECHO [BUILD] Building %CDC_CONSUMER% image ...
    docker build -t %CDC_CONSUMER% -f dockerfile-cdc-consumer.dockerfile .
    ECHO [INFO] %CDC_CONSUMER% image build completed.
)

ECHO [UP] Starting containers ...
docker compose up -d
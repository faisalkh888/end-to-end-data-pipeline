ALTER ROLE :"postgres_user" WITH LOGIN PASSWORD :'postgres_password';
ALTER ROLE airflow WITH LOGIN PASSWORD :'airflow_password';
ALTER ROLE loader WITH LOGIN PASSWORD :'loader_password';
ALTER ROLE extractor WITH LOGIN PASSWORD :'extractor_password';

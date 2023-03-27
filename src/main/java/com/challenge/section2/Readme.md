docker-compose -f docker-compose-postgres.yml up

docker exec -it scripts-db-1 /bin/sh -c " psql -U postgres -d [database_name] -f setup.sql "

sql > SELECT table_schema,table_name
FROM information_schema.tables
ORDER BY table_schema,table_name; 

// check existing database and tables


curl -v -d "{\"name\": \"bikename\",\"status\":1}" -H "Content-Type:application/json" http://localhost:5000/v1.0/vehicles

### Deploy with Zappa

cd src

zappa init

zappa deploy dev

Then add the postgresql connection parameters in the AWS Lambda environment variables:

- PGDATABASE
- PGHOST
- PGPORT
- PGUSER
- PGPASSWORD
# smb-backend

## Installation

-  Clone this repo
-  Use pip to install the code:

   ```
   pip install --editable .
   ```
   
   (The `--editable` flag is optional, but very useful in development)
   

## AWS lambdas deployment

In a nutshell, run:

```
zappa deploy smbbackend
set-lambda-env smbbackend
```

The lambdas can be deployed with [zappa][zappa]. Check the 
`zappa_settings.json` file for more details on available stages.

Deploy a lambda to aws with the following:

```shell
zappa deploy smbbackend  # first time deployment
zappa update smbbackend  # subsequent deployments
zappa undeploy smbbackend  # remove deployment

```

### Environment variables

The AWS console can be used to set environment variables for each lambda 
function. In addition, the `smbbackend/awsutils.py` file may also be used. 
It works by looking at the current local environment and searching for 
variables with a special naming. These vars are pushed to the lambda's 
environment.

This file is also installed as a console script named `set-lambda-env` when 
you install this package with pip.


-  Set local environment variables with the following naming convention:

   ```shell
   ZAPPA_SMBBACKEND_<VARIABLE-NAME>=<VARIABLE_VALUE>

   ```

-  Run the script, providing the name of the zappa stage an argument
   
   
Example:

For setting the `DB_HOST=somehost` env variable for the 
`savemybike-ingesttrack-dev` lambda, you would do the following:

```shell
export ZAPPA_SAVEMYBIKE_INGESTTRACK_DEV_DB_HOST=somehost
set-lambda-env.py smbbackend
```

Alternatively you could define all environment files in a file, then export
all variables to the current environment and finally run the script


#### Note

The current zappa configuration **expects** to find the following environment
variables:

```shell
AWS_ACCESS_KEY_ID="your-own-aws-access-key-id"
AWS_SECRET_ACCESS_KEY="your-own-secret-access-key"
AWS_DEFAULT_REGION="aws-region-to-use"
```

Although it is possible to specify these in the zappa configuration file, we
keep their definition outside of it in order to keep the config more portable


[zappa]: https://github.com/Miserlou/Zappa


## Using test data

The `tests/data` directory contains a spatialite DB with some test data. This
data can be loaded by running:

```
convert-spatialite --output_dir /tmp tests/data/smb_test_data.sqlite

DB_HOST="<db-host>" \
DB_NAME="<db-name>" \
DB_USER="<db-user>" \
DB_PASSWORD="<db-password>" \
DB_PORT="<db-port>" \
ingest-tracks <uuid-of-the-user-who-will-own-tracks> /tmp/track_1.csv /tmp/track_2.csv

```

The `convert-spatialite` and `inges-tracks` scripts become available when the 
package is installed via pip. They can be combined in order to provide test
data to multiple users
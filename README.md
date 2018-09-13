# smb-backend

## AWS lambdas deployment

The lambdas can be deployed with [zappa][zappa]. The following zappa stages 
are available: 

-  ingesttrack_dev
-  calculateindexes_dev
-  calculatebadges_dev
-  calculateprizes_dev

check the `zappa_settings.json` file for more details on these.

Deploy a lambda to aws with the following:

```shell
zappa deploy ingestrack_dev  # first time deployment
zappa update ingestrack_dev  # subsequent deployments
zappa undeploy ingestrack_dev  # remove deployment

```

### Environment variables

The AWS console can be used to set environment variables for each lambda 
function. In addition, the `scripts/setlambdaenv.py` file may also be used. 
It works by looking at the current local environment and searching for 
variables with a special naming. These vars are pushed to the lambda's 
environment.

How to use it:

-  Set local environment variables with the following naming convention:

   ```shell
   ZAPPA_<LAMBDA-FUNCTION-NAME>_<VARIABLE-NAME>=<VARIABLE_VALUE>

   ```

-  Run the script, providing the name of the lambda function as an argument - 
   Note that lambdas deployed by zappa are named `<projectname-stagename>`
   
   
Example:

For setting the `DB_HOST=somehost` env variable for the 
`savemybike-ingesttrack-dev` lambda, you would do the following:

```shell
export ZAPPA_SAVEMYBIKE_INGESTTRACK_DEV_DB_HOST=somehost
python setlambdaenv.py savemybike-ingesttrack-dev
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
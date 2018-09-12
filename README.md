# smb-backend

## AWS lambdas deployment

The lambdas can be deployed with [zappa][zappa]. The following zappa stages 
are available: 

-  ingesttrack_dev

check the `zappa_settings.json` file for more details on these.

Deploy a lambda to aws with the following:

```shell
zappa deploy ingestrack_dev  # first time deployment
zappa update ingestrack_dev  # subsequent deployments
zappa undeploy ingestrack_dev  # remove deployment

```

#### Note

The current zappa configuration **expects** to find the following environment
variables:

```shell
AWS_ACCESS_KEY_ID="your-own-aws-access-key-id"
AWS_SECRET_ACCESS_KEY="your-own-secret-access-key"
```

[zappa]: https://github.com/Miserlou/Zappa
Primer App Config Overrides
===========================

This is the directory for deployment and infrastructure configs. At the time of any new primer app creation, configs (only deployment.json gets created).

There are two types of configuration files that resides under .primer directory :-
 - App specific infrastructure config for all envs/regions/zones resides in infrastructure.json.
 - App Specific deployment config for all envs resides in deployment.json.
 
Levis Service will serve as a new configuration management. It has default values for all of the configs that are required for any primer app deployment. Primer apps can override 
those configurations here.

The default values for all configs served by Levis are at following location :-
 - [infrastructure](https://ewegithub.sb.karmalab.net/EWE/levis-lambda/tree/master/src/deployment/config) 
 - [deployment](https://ewegithub.sb.karmalab.net/EWE/levis-lambda/tree/master/src/infrastructure/config)

The allowed environments are following :-
 - ewetest
 - ewetest-int
 - ewetest-stress
 - eweprod
 - eweprod-p
 - bigdataprod
 
## Sample for deployment config (deployment.json)
 - **Global** - defines the parameters which are common for different environment.
 - **Env** - All overrides for a specific environment go under specific environment key. 
 - If any field needs to be overridden for a particular environment, then that fields needs to be provided under that environment key.

```javascript
{
  "global": {
    "app_type": "expressjs",
    "isactive_support": {
      "enabled": true, // This needs to be set to true if one wants to override the isActive endpoint.
      "file": "/opt/primer-ui-web/active.txt" // The isActive check can be overriden. This is useful for the deployers to check whether the app is up.
    },
    "source_repo": "git@ewegithub.sb.karmalab.net:EWE/primer-ui-web.git",
    "team": "Primer UI Devs",
    "department": "Exp Wotif Tech Platform Austra",
    "cost_center": "10496",
    "portfolio": "Shared - Internal Tools",
    "product_area": "Platform",
    "notify": {
      "hipchat": {
        "room": "<global hipchat_room>"
      },
      "email": {
        "addresses": [
          "primer-ui-devs@expedia.com"
        ]
      }
    }
  },
  "eweprod-p": {
    "instances": {
        "min_count": 3,
        "max_count": 4
      }
  },
  "eweprod": {
    "app_ssl_port": 8080, 
    "app_ssl_protocol": "HTTP", 
    "loadbalancer": {
      "https_enabled": true //If one wants to enable https on load balancer, this field needs to be enabled.
    }
  },
  "ewetest-int": {
    "app_ssl_port": 8080,
    "app_ssl_protocol": "HTTP",
    "ecs": {
      "max_mem": 3072, // maximum memory of the container
      "min_cpu": 2048  // minimum cpu of the container
    },
    "no-auto-delete": true,  // if auto delete wanted to be turned off for any environment.
    "loadbalancer": {
      "https_enabled": true,
      "timeout": 60
    }
  }
}
```

## Sample for infrastructure config (infrastructure.json)

- **Global** - defines the parameters which are common for different environment.
- **Env** - Other env/zone specific overrides can be added to under respective env keys.
- It is highly encouraged to not override subnet, security groups unless there is a valid reason.

```javascript
{
  "eweprod": {
    "us-east-1a": {
      "elb_scheme": "internet-facing", // The default value is internal.
      "elb_https_certificate_arn": "<valid elb https certificate arn>"  // The default value can be checked in levis for this environment
      "elb": {
        "security_group_ids": "sg-9c211af8,sg-8d9f90e9,sg-f8967d97,sg-d727c8b8", // This needs to replaced completely if one wants to add a new or remove a security group.
        "sticky_session_timeout": "3600" // The default value is 1800 for all envs
      }
    },
    "us-east-1d": {
      "elb": {
        "security_group_ids": "sg-9c211af8,sg-8d9f90e9,sg-f8967d97,sg-d727c8b8"
      }
    }
  },
  "ewetest-int": {
    "us-east-1a": {
      "security_group_ids": "sg-b4618edb,sg-45608f2a,sg-44608f2b,sg-30f41a5f,sg-554e8c3a,sg-f746a998"
    },
    "us-west-2a": {
      "elb": {
        "security_group_ids": "sg-5b36f13f,sg-124c3d77,sg-51588334"
      }
    }
  },
  "ewetest-stress": {
    "us-east-1d": {
      "size": "c3.large" // The size of the instance
    }
  },
  "ewetest": {
    "us-east-1d": {
      "security_group_ids": "sg-b4618edb,sg-45608f2a,sg-44608f2b,sg-30f41a5f,sg-554e8c3a,sg-f746a998"
    },
    "us-west-2c": {
      "elb_https_certificate_arn": "arn:aws:iam::408096535527:server-certificate/wildcard.us-west-2.test.expedia.com", // elb https certificate arn
      "elb": {
        "security_group_ids": "sg-5b36f13f,sg-124c3d77,sg-51588334"
      }
    }
  }
}
```

 - For any other information on overriding primer app configs, please take a look at [Confluence](https://confluence/display/PRIMER/Primer+App+Config+Overrides) or contact hipchat **Primer**.

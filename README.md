# bd_metrics_monitor

### Env Setup

```shell
pyenv virtualenv 3.4.0 bd_metrics_monitor
pyenv local bd_metrics_monitor
```

### Package Dependencies

```
certifi==2018.4.16
chardet==3.0.4
idna==2.6
psycopg2==2.7.4
requests==2.18.4
urllib3==1.22
```

### Function List

- Pull data source from Github
- Loading csv section
- Import data source to database
- Compare different fields
- Dingidng bot alarm
- PostgreSQL trigger function

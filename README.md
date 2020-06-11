# 15five-connect
Quick script to dump 15five data generated after a certain date and load it into a postgres database

# environment variables
The following environment variables need to be set:

```
export FFIVE_DBHOST="<mydatabasehostname>"
export FFIVE_DBNAME="<mydatabasename>"
export FFIVE_DBUSER="<mypostgresusername>"
export FFIVE_DBPASS="<mypostgrespassword>"
export FFIVE_URL="https://<mycompany>.15five.com/api/public"
export FFIVE_TOKEN="<my15fiveapitoken>"
```

# usage
python 15five-connect.py --date YYYY-MM-DD

# dynamodb-add-ttl
Adds a TTL attribute to a table for existing data based on an existing attribute and provided expiry time.  Uses parallel scans for performance.

# Purpose
DynamoDB TTLs are a great feature that allow auto-pruning of data from tables.  However, ttls must be in epoch time and unless your application is already writing, you'll need to backfill and add a tll to existing records.  That's the purpose of this tool.

# Usage

```
python dynamodb-add-ttl.py -d <ttl duration in days> -m <attribute to base ttl on> -n <name of new ttl attribute to add> -r <region> -s <number of segments/threads> -t <table name>
```

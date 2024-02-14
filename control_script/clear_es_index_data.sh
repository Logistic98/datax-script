curl -u username:password -XPOST 'http://ip:port/index_name/_delete_by_query?refresh&slices=5&pretty' -H 'Content-Type: application/json' -d'{"query": {"match_all": {}}}'

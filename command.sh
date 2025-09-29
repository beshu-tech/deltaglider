export AWS_ENDPOINT_URL=http://localhost:9000
export AWS_ACCESS_KEY_ID=deltadmin
export AWS_SECRET_ACCESS_KEY=deltasecret

ror-data-importer \
  --source-bucket=dg-demo \
  --dest-bucket=new-buck \
  --yes
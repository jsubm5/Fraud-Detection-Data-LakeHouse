# load environemnt.env
export $(grep -v '^\s*#.*' ./environment.env | xargs)

# creating nessie data
docker compose up dremio -d

# create default user
docker exec -u root dremio /bin/bash -c " \
echo 'debug:{
  addDefaultUser: true
}' >> \
/opt/dremio/conf/dremio.conf"

# in orderder for new configurations to take effect
docker restart dremio

sleep 60s

# Now you can login with:
# username: dremio
# password: dremio123

# getting credentials
response=$(curl -s -X POST "$DREMIO_ENDPOINT/apiv2/login" \
    --header "Content-Type: application/json" \
    --data-raw "{
    \"userName\": \"$DREMIO_USERNAME\",
    \"password\": \"$DREMIO_PASSWORD\"
    }")
echo $response
token=$(echo "$response" | grep -oP '"token":\s*"\K[^"]+')
echo $token


# creating the source
response=$(curl -X POST "$DREMIO_ENDPOINT/api/v3/catalog" \
    --header "Authorization: Bearer $token" \
    --header "Content-Type: application/json" \
    --data-raw "{
        \"entityType\": \"source\",
        \"config\": {
            \"nessieEndpoint\": \"http://nessie:19120/api/v2\",
            \"nessieAuthType\": \"NONE\",
            \"asyncEnabled\": true,
            \"isCachingEnabled\": true,
            \"maxCacheSpacePct\": 100,
            \"credentialType\": \"ACCESS_KEY\",
            \"awsAccessKey\": \"$MINIO_ACCESS_KEY\",
            \"awsAccessSecret\": \"$MINIO_SECRET_KEY\",
            \"awsRootPath\": \"/warehouse\",
            \"propertyList\": [
                {
                    \"name\": \"fs.s3a.path.style.access\",
                    \"value\": \"true\"
                },
                {
                    \"name\": \"fs.s3a.endpoint\",
                    \"value\": \"minio:9000\"
                },
                {
                    \"name\": \"dremio.s3.compat\",
                    \"value\": \"true\"
                }
            ],
            \"secure\": false
        },
        \"name\": \"$NESSIE_CATALOG_NAME\",
        \"type\": \"NESSIE\"
    }")

echo $response

# creating spaces and views
python ./Dremio/dremio_init.py

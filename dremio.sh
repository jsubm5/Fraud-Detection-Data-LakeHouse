    username=ahmadMu
    password=passw0rd
    dremioserver=http://localhost:9047

    # create_view --name 'my view' --space 'Bronz' --query 'SELECT *'
    response=$(curl -s -X POST "$dremioserver/apiv2/login" \
        --header "Content-Type: application/json" \
        --data-raw "{
        \"userName\": \"$username\",
        \"password\": \"$password\"
        }")
    echo $response
    token=$(echo "$response" | grep -oP '"token":\s*"\K[^"]+')
    echo $token
    path='BRONZ'
    name='ccc'
    query='SELECT 1;'
    response=$(curl -X POST "$dremioserver/api/v3/catalog" \
        --header "Authorization: Bearer $token" \
        --header 'Content-Type: application/json' \
        --data-raw "{
        \"entityType\": \"dataset\",
        \"path\": [
            \"$path\",
            \"$name\"
        ],
        \"type\": \"VIRTUAL_DATASET\",
        \"sql\": \"$query\"
        }")
    if echo "$response" | grep -q '"error"'; then
        echo "Error: Failed to create view. Response: $response"
        return 1
    else
        echo "view '$name' created successfully."
    fi

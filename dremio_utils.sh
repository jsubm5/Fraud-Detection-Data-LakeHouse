#!/bin/bash

# token=""
# dremioServer= ""

function get_user_token {
    while [[ "$#" -gt 0 ]]; do
        case $1 in
        --server)
            dremioserver="$2"
            shift
            ;;
        --username)
            username="$2"
            shift
            ;;
        --password)
            password="$2"
            shift
            ;;
        *)
            echo "Unknown parameter passed: $1"
            exit 1
            ;;
        esac
        shift
    done

    response=$(curl -s -X POST "$dremioserver/apiv2/login" \
        --header "Content-Type: application/json" \
        --data-raw "{
    \"userName\": \"$username\",
    \"password\": \"$password\"
    }")

    token=$(echo "$response" | grep -oP '"token":\s*"\K[^"]+')
    if [[ -z "$token" ]]; then
        echo "Error: Failed to retrieve token. Please check your credentials and try again."
        return 1
    else
        echo "Token: $token"
    fi
}

function create_space {
    while [[ "$#" -gt 0 ]]; do
        case $1 in
        --name)
            name="$2"
            shift
            ;;
        --description)
            description="${2:- No description has been provided}"
            shift
            ;;
        *)
            echo "Unknown parameter passed: $1"
            exit 1
            ;;
        esac
        shift
    done

    response=$(curl -s -X POST "$dremioServer/api/v3/catalog" \
        --header "Authorization: Bearer $token" \
        --header "Content-Type: application/json" \
        --data "{
    \"entityType\": \"space\",
    \"name\": \"$name\",
    \"description\": \"$description\"
    }")

    # Check for errors in the response
    if echo "$response" | grep -q '"error"'; then
        echo "Error: Failed to create space. Response: $response"
        return 1
    else
        echo "Space '$name' created successfully."
    fi
}

function create_view {
    while [[ "$#" -gt 0 ]]; do
        case $1 in
        --name)
            name="$2"
            shift
            ;;
        --path)
            space="$2"
            shift
            ;;
        --path)
            query="$2"
            shift
            ;;
        *)
            echo "Unknown parameter passed: $1"
            exit 1
            ;;
        esac
        shift
    done

    response=$(curl -X POST "$dremioServer/api/v3/catalog" \
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
        echo "Error: Failed to create space. Response: $response"
        return 1
    else
        echo "Space '$name' created successfully."
    fi
}

#!/bin/bash

API_KEY='$2b$10$WnTrwWKzFU/Yutf4NWKLUeC9mVBvx7dyD3qH8tp33KE5uVQXd4GXK'
COLLECTION_ID='64d9e43d9d312622a390ec7d'

curl -XPOST \
    -H "Content-type: application/json" \
    -H "X-Master-Key: $API_KEY" \
    -H "X-Collection-Id: $COLLECTION_ID" \
    -d @dogs.json \
    "https://api.jsonbin.io/v3/b"

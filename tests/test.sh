#!/bin/bash

curl -s -X POST \
  -H "Content-Type: application/json" \
  -d $'{
    "input": {
      "image": "https://replicate.delivery/pbxt/KhTOXyqrFtkoj2hobh1a4As6dYDIvNV2Ujbc0LbGD9ZguRwR/bowers.jpg"
    }
  }' \
  http://localhost:8000/c/text-extract-ocr/predictions

curl -s -X POST \
  -H "Content-Type: application/json" \
  -d $'{
    "input": {
      "task": "image_captioning",
      "image": "https://replicate.delivery/mgxm/f4e50a7b-e8ca-432f-8e68-082034ebcc70/demo.jpg"
   }
  }' \
  http://localhost:8000/c/blip/predictions

echo "Done"
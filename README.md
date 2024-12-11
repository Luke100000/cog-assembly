# Cog-Assembly

Simple, single system docker manager for http services with focus on VRAM scheduling.
Connecting to a cold container boots up the container transparently and forwards the request to it.
Recommended for [Cog inference](https://github.com/replicate/cog) but generally every http service can be managed.

## Installation

```bash
git clone https://github.com/Luke100000/cog-assembly.git
cd cog-assembly
poetry install
uvicorn main:app
```

## Configuration

[`config.yaml`](config_template.yaml) contains the configuration, the template contains an example and documentation.

## Usage

The service is now exposed under `/c/<service>/<endpoint>`.

```bash
curl -s -X POST \
  -H "Content-Type: application/json" \
  -d $'{
    "input": {
      "image": "https://replicate.delivery/pbxt/KhTOXyqrFtkoj2hobh1a4As6dYDIvNV2Ujbc0LbGD9ZguRwR/bowers.jpg"
    }
  }' \
  http://localhost:8000/c/text-extract-ocr/predictions
```

## Authentication

The Bearer token is used to authenticate requests.
Configure users in the config.
If no token is provided, the `default` user is used.
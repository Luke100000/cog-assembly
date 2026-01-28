# Cog-Assembly

Simple, single system docker manager for http services with focus on RAM and VRAM scheduling.
Connecting to a cold container boots up the container transparently and forwards the request to it.
Recommended for [Cog inference](https://github.com/replicate/cog) but generally every http service can be managed.
Comes with an admin panel, dashboard, metrics, and OpenAPI compatible UI.

## Installation

Either use Docker:

```bash
docker run -p 8000:8000 -v ./data:/data luke100000/cog-assembly
```

Or install from source:

```bash
git clone https://github.com/Luke100000/cog-assembly.git
cd cog-assembly

uv sync
uv run uvicorn app.main:app
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
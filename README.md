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

`settings.yaml` contains the configuration for the manager.
```yaml

```

`services.yaml` contains the configuration for the services.
```yaml
```

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

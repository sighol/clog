# Parsing of JSON logs

If you have a service that outputs logs in JSON format so that GCP can read it,
then you can pipe the output through this program to make it readable again

## Installation

```sh
cargo install --git https://github.com/sighol/clog
```

## Usage

```
kubectl logs POD_NAME CONTAINER_NAME --follow | clog
```

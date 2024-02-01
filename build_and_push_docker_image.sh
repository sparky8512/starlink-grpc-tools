set -o errexit -o xtrace
pushd "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
docker build --platform linux/amd64 -t danleone/starlink-grpc-tools:latest -f Dockerfile .
docker push danleone/starlink-grpc-tools:latest

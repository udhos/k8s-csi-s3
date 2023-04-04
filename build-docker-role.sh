#!/bin/bash

version=0.35.0r ;# r stands for role

tag=udhos/csi-s3-driver:$version

docker build -t $tag -f Dockerfile.role .

echo "# test s3fs:"
echo docker run --entrypoint /usr/bin/s3fs -ti --rm $tag

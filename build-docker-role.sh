#!/bin/bash

version=0.35.0r ;# r stands for role

docker build -t udhos/csi-s3:$version -f Dockerfile.role . 

echo "# test s3fs:"
echo docker run --entrypoint /usr/bin/s3fs -ti --rm udhos/csi-s3-driver:0.35.0r

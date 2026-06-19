#!/bin/bash

bash minIO/scripts/init_bucket.sh

python3 minIO/scripts/init_namespace.py

bash config/dockerfile/build_images.sh
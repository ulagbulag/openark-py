# Copyright (c) 2022 Ho Kim (ho.kim@ulagbulag.io). All rights reserved.
# Use of this source code is governed by a GPL-3-style license that can be
# found in the LICENSE file.

# Configure environment variables
ARG PYTHON_VERSION="3.12-bookworm"

# Be ready for serving
FROM "docker.io/library/python:${PYTHON_VERSION}" as server

# Install dependencies
RUN apt-get update && apt-get install -y \
    # Install core dependencies
    findutils \
    # Install build dependencies
    cargo && \
    # Cleanup
    rm -rf /var/lib/apt/lists/*

# Install it as a package
ADD ./ /usr/src
WORKDIR /usr/src
RUN pip install . && \
    # Cleanup
    rm -rf /usr/src && \
    find /usr -type d -name '*__pycache__' -prune -exec rm -rf {} \;

# Cleanup
WORKDIR /

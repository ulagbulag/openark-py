# Copyright (c) 2022 Ho Kim (ho.kim@ulagbulag.io). All rights reserved.
# Use of this source code is governed by a GPL-3-style license that can be
# found in the LICENSE file.

# Configure environment variables
ARG PYTHON_VERSION="3.12-bookworm"

# Be ready for serving
FROM "docker.io/library/python:${PYTHON_VERSION}" as server

# Install dependencies
# FIXME: detach builder from server
ADD ./requirements.txt /requirements.txt
RUN apt-get update && apt-get install -y \
    # Install core dependencies
    cargo \
    # Install build dependencies
    findutils && \
    # Install python build dependencies
    python -m pip install --no-cache-dir setuptools_rust && \
    # Install python dependencies
    python -m pip install --no-cache-dir --requirement /requirements.txt && \
    # Cleanup
    find /usr -type d -name '*__pycache__' -prune -exec rm -rf {} \; && \
    rm /requirements.txt && \
    rm -rf /var/lib/apt/lists/*

# Add source codes
ADD ./ /opt/openark
WORKDIR /opt/openark

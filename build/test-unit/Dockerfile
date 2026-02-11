# Copyright 2023 LiveKit, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# syntax=docker/dockerfile:1.6
ARG TARGETPLATFORM
ARG TARGETARCH
ARG GO_VERSION=1.24.5

FROM livekit/gstreamer:1.24.12-dev

ARG TARGETPLATFORM
ARG TARGETARCH
ARG GO_VERSION

WORKDIR /workspace

RUN apt-get update && RUN apt-get update && \
apt-get install -y \
    curl \
    ffmpeg \
    fonts-noto \
    gnupg \
    pulseaudio \
    python3 \
    python3-pip \
    unzip \
    wget \
    xvfb \
    git-lfs \
    gstreamer1.0-plugins-base-

# Install Go
RUN wget --no-check-certificate https://go.dev/dl/go${GO_VERSION}.linux-${TARGETARCH}.tar.gz && \
    rm -rf /usr/local/go && \
    tar -C /usr/local -xzf go${GO_VERSION}.linux-${TARGETARCH}.tar.gz && \
    rm go${GO_VERSION}.linux-${TARGETARCH}.tar.gz
ENV PATH="/usr/local/go/bin:${PATH}"

ENV GOMODCACHE=/go/pkg/mod \
    GOCACHE=/go/build-cache

# Download go modules
COPY go.mod .
COPY go.sum .
RUN --mount=type=cache,target=/go/pkg/mod,sharing=locked \
    --mount=type=cache,target=/go/build-cache \
    go mod download

RUN go install github.com/magefile/mage@latest

# Copy source code
COPY pkg/ pkg/
COPY cmd/ cmd/
COPY version/ version/

CMD ["tail", "-f", "/dev/null"]
FROM ubuntu:24.04
# Pin to the latest LTS version of Ubuntu image (next LTS release is in April 2026) 
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/
# TODO: use alpine image for smaller image size and faster builds

ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y make

# Set working directory and copy the project code
WORKDIR /app
COPY . /app

# Clean up to make the image smaller
RUN rm -rf /var/lib/apt/lists/* && \
    apt-get clean

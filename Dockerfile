# syntax=docker/dockerfile:1

# ======= build stage =======
FROM --platform=$BUILDPLATFORM golang:alpine as builder
ARG TARGETOS
ARG TARGETARCH

RUN apk update && apk add --no-cache git

# setup support for private modules
ARG GITHUB_TOKEN
ARG GITHUB_TOKEN_OWNER

# create a .netrc file for GitHub authentication
RUN echo "machine github.com\nlogin ${GITHUB_TOKEN_OWNER}\npassword ${GITHUB_TOKEN}" > /root/.netrc 

RUN cat /root/.netrc 

# set correct permissions for the .netrc file
RUN chmod 600 /root/.netrc 

RUN git config --global --add url."https://${GITHUB_TOKEN_OWNER}:${GITHUB_TOKEN}@github.com".insteadOf "https://github.com"
ENV GOPRIVATE=github.com/orasis-holding

WORKDIR /app

# Fetch dependencies.
COPY go.mod go.sum ./
RUN go mod download && go mod verify

RUN mkdir ./tmp

COPY . .


RUN --mount=target=. \
    --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -ldflags='-w -s -extldflags "-static"' -a -o /go/bin/consumer ./main.go

# ======= release stage =======
FROM gcr.io/distroless/static-debian11 as release

USER nonroot:nonroot
# Import from builder.
COPY --from=builder --chown=nonroot:nonroot /go/bin/consumer /go/bin/consumer

ENTRYPOINT ["/go/bin/consumer"]

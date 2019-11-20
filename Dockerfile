FROM golang:1

ENV PROJECT=generic-rw-s3

ENV ORG_PATH="github.com/Financial-Times"
ENV BUILDINFO_PACKAGE="${ORG_PATH}/service-status-go/buildinfo."

COPY . /${PROJECT}/
WORKDIR /${PROJECT}/

RUN VERSION="version=$(git describe --tag --always 2> /dev/null)" \
  && DATETIME="dateTime=$(date -u +%Y%m%d%H%M%S)" \
  && REPOSITORY="repository=$(git config --get remote.origin.url)" \
  && REVISION="revision=$(git rev-parse HEAD)" \
  && BUILDER="builder=$(go version)" \
  && LDFLAGS="-X '"${BUILDINFO_PACKAGE}$VERSION"' -X '"${BUILDINFO_PACKAGE}$DATETIME"' -X '"${BUILDINFO_PACKAGE}$REPOSITORY"' -X '"${BUILDINFO_PACKAGE}$REVISION"' -X '"${BUILDINFO_PACKAGE}$BUILDER"'" \
  && CGO_ENABLED=0 go build -mod=readonly -o /artefacts/${PROJECT} -ldflags="${LDFLAGS}"

FROM scratch
WORKDIR /
COPY --from=0 /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=0 /artefacts/* /

CMD ["/generic-rw-s3"]

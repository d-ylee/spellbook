ARG go_version

FROM golang:${go_version}

WORKDIR /opt/hay

COPY go.mod ./
COPY go.sum ./
COPY hay ./

RUN go mod download
RUN go build -o /hay


CMD [ "/hay" ]

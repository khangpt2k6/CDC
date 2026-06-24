module github.com/khangpt2k6/CDC

go 1.26

tool (
	google.golang.org/grpc/cmd/protoc-gen-go-grpc
	google.golang.org/protobuf/cmd/protoc-gen-go
)

require google.golang.org/protobuf v1.36.11

require google.golang.org/grpc/cmd/protoc-gen-go-grpc v1.6.2 // indirect

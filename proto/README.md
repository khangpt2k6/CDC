# proto/

Protobuf contract for the CDC platform (package `cdc.v1`).

The canonical change-event envelope and, later, the control-plane gRPC service
live here. Lint, breaking-change detection, and Go codegen are driven by `buf`
(see `buf.yaml` / `buf.gen.yaml`). Generated Go bindings are emitted into
`internal/gen/`.

module godror_test

go 1.17

//replace github.com/godror/godror => ../github.com/godror/godror

require github.com/godror/godror v0.34.1-0.20221101093820-c131a621b1a8

require (
	github.com/go-logfmt/logfmt v0.5.1 // indirect
	github.com/go-logr/logr v1.2.3 // indirect
	github.com/godror/knownpb v0.1.0 // indirect
	google.golang.org/protobuf v1.28.1 // indirect
)

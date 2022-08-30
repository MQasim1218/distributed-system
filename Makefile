server:
	go run ./Cmd/main.go

proto:
	protoc Api/v1/*.proto --go_out=. --go_opt=paths=source_relative --proto_path=.
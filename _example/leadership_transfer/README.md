# leadership transfer

session 1 (run [LeadershipTransfer](https://pkg.go.dev/github.com/octu0/bully-election#Bully.LeadershipTransfer) after 10s)

```
$ go run . -id node1 -addr 127.0.0.1 -port 6351 -join 127.0.0.1:6351 -transfer
```

session 2

```
$ go run . -id node2 -addr 127.0.0.1 -port 6352 -join 127.0.0.1:6351
```

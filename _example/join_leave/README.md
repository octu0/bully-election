# join/leave

session 1

```
$ go run . -id node1 -addr 127.0.0.1 -port 6351 -join 127.0.0.1:6351
```

session 2

```
$ go run . -id node2 -addr 127.0.0.1 -port 6352 -join 127.0.0.1:6351
```

session 3

```
$ go run . -id node3 -addr 127.0.0.1 -port 6353 -join 127.0.0.1:6351 -nonvoter
```

session 4
```
$ go run . -id node4 -addr 127.0.0.1 -port 6354 -join 127.0.0.1:6351
```
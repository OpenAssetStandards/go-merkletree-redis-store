# Go Merkle SQL Redis Store

## Usage 

```go
rdb := redis.NewClient(&redis.Options{
    Addr:     "localhost:6379",
    Password: "", // no password set
    DB:       0,  // use default DB
})
mt, err := NewMerkleTree(merkleredis.NewMerkleRedisStorage(rdb, "my-prefix"), 10)
```


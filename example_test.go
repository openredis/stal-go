package stal_test

import (
	"fmt"
	"log"
	"sort"

	"github.com/gomodule/redigo/redis"
	stal "github.com/openredis/stal-go"
)

func Example() {
	pool := &redis.Pool{
		Dial: func() (redis.Conn, error) {
			return redis.DialURL("redis://localhost:6379")
		},
	}

	stal, err := stal.New(pool)
	if err != nil {
		log.Fatal(err)
	}

	conn := pool.Get()
	defer conn.Close()

	conn.Send("MULTI")
	conn.Send("FLUSHDB")
	conn.Send("SCRIPT", "FLUSH")
	conn.Send("SADD", "foo", "a", "b", "c")
	conn.Send("SADD", "bar", "b", "c", "d")
	conn.Send("SADD", "baz", "c", "d", "e")
	conn.Send("SADD", "qux", "x", "y", "z")
	if _, err := conn.Do("EXEC"); err != nil {
		log.Fatal(err)
	}

	got, err := redis.Strings(stal.Solve("SINTER", "foo", "bar"))
	if err != nil {
		log.Fatal(err)
	}
	sort.Strings(got)

	fmt.Printf("%v", got)
	// Output: [b c]
}

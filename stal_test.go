package stal

import (
	"os"
	"reflect"
	"sort"
	"testing"

	"github.com/gomodule/redigo/redis"
)

func TestNew(t *testing.T) {
	pool := mustRedisPool(t)
	defer pool.Close()

	stal, err := New(pool)
	if err != nil {
		t.Fatal(err)
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
		t.Fatal(err)
	}

	t.Run("example expression", func(t *testing.T) {
		got, err := redis.Strings(stal.Solve("SUNION", "qux", []interface{}{"SDIFF", []string{"SINTER", "foo", "bar"}, "baz"}))
		if err != nil {
			t.Fatal(err)
		}
		sort.Strings(got)

		want := []string{"b", "x", "y", "z"}
		if !reflect.DeepEqual(want, got) {
			t.Fatalf("wanted stal result %v, got %v", want, got)
		}
	})

	t.Run("commands without sub expressions also work", func(*testing.T) {
		got, err := redis.Strings(stal.Solve("SINTER", "foo", "bar"))
		if err != nil {
			t.Fatal(err)
		}
		sort.Strings(got)

		if want := []string{"b", "c"}; !reflect.DeepEqual(want, got) {
			t.Fatalf("wanted stal result %v, got %v", want, got)
		}
	})

	t.Run("verify there's no keyspace pollution", func(t *testing.T) {
		got, err := redis.Strings(conn.Do("KEYS", "*"))
		if err != nil {
			t.Fatal(err)
		}
		sort.Strings(got)
		if want := []string{"bar", "baz", "foo", "qux"}; !reflect.DeepEqual(want, got) {
			t.Fatalf("wanted keys %v, got %v", want, got)
		}
	})
}

func mustRedisPool(t testing.TB) *redis.Pool {
	t.Helper()

	u, err := getenv("REDIS_URL", "redis://localhost:6379")
	if err != nil {
		t.Skip(err.Error())
	}

	return &redis.Pool{
		Dial: func() (redis.Conn, error) {
			return redis.DialURL(u)
		},
	}
}

func getenv(key, fallback string) (string, error) {
	if v := os.Getenv(key); v != "" {
		return v, nil
	}
	return fallback, nil
}

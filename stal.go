package stal

import (
	"encoding/json"

	"github.com/gomodule/redigo/redis"
	"github.com/pkg/errors"
)

// Stal is a simple wrapper around https://github.com/soveran/stal.
type Stal struct {
	pool   *redis.Pool
	script *redis.Script
}

// New creates the Stal wrapper, and pre-loads the script. If the preloading
// fails, an error is returned.
func New(pool *redis.Pool) (*Stal, error) {
	conn := pool.Get()
	defer conn.Close()

	script := redis.NewScript(0, stalLua)
	if err := script.Load(conn); err != nil {
		return nil, errors.Wrap(err, "SCRIPT LOAD failed")
	}

	return &Stal{pool: pool, script: script}, nil
}

// Solve takes an expression, and returns the result. The result maps exactly
// to redigo's redis reply types, and the user is expected to cast these values
// as they see fit.
func (s *Stal) Solve(args ...interface{}) (interface{}, error) {
	expr, err := json.Marshal(args)
	if err != nil {
		return nil, errors.Wrap(err, "json.Marshal failed")
	}

	conn := s.pool.Get()
	defer conn.Close()

	reply, err := s.script.Do(conn, expr)
	return reply, errors.Wrap(err, "Stal script failed")
}

const stalLua = `
-- Copyright (c) 2016 Michel Martens

local expr = cjson.decode(ARGV[1])

local tr = {
  SDIFF  = "SDIFFSTORE",
  SINTER = "SINTERSTORE",
  SUNION = "SUNIONSTORE",
  ZINTER = "ZINTERSTORE",
  ZUNION = "ZUNIONSTORE",
}

local function append(t1, t2)
  for _, item in ipairs(t2) do
    table.insert(t1, item)
  end
end

local function map(t, f)
  local nt = {}

  for k, v in pairs(t) do
    nt[k] = f(v)
  end

  return nt
end

local compile, convert

function compile(expr, ids, ops)
  return map(expr, function(v)
    if (type(v) == "table") then
      return convert(v, ids, ops)
    else
      return v
    end
  end)
end

function convert(expr, ids, ops)
  local tail = {unpack(expr)}
  local head = table.remove(tail, 1)

  -- Key where partial results will be stored
  local id = "stal:" .. #ids

  -- Keep a reference to clean it up later
  table.insert(ids, id)

  -- Translate into command and destination key
  local op = {tr[head] or head, id}

  -- Compile the rest recursively
  append(op, compile(tail, ids, ops))

  -- Append the outermost operation
  table.insert(ops, op)

  return id
end

local function solve(expr)
  local ids = {}
  local ops = {}
  local res = nil

  table.insert(ops, compile(expr, ids, ops))

  if (#ops == 1) then
    return redis.call(unpack(ops[1]))
  else
    for _, op in ipairs(ops) do
      if (#op > 1) then
        res = redis.call(unpack(op))
      end
    end

    redis.call("DEL", unpack(ids))

    return res
  end
end

if redis.replicate_commands then
  redis.replicate_commands()
  redis.set_repl(redis.REPL_NONE)
end

return solve(expr)
`

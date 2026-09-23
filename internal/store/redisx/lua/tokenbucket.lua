-- tokenbucket.lua: token bucket with lazy refill. State is a 2-field hash:
-- t = micro-tokens (one token = 1e6), l = last refill instant in unix
-- microseconds. A missing key is a full bucket. Refill is computed in two
-- integer parts (whole emission intervals, then the remainder scaled to
-- micro-tokens); whole >= burst short-circuits to full so no product
-- approaches 2^53. Last never moves backwards. Reference model: internal/algo/tokenbucket.Step.
-- Table shape (read/eval/write): see gcra.lua.
local MT = 1000000

-- tofull is the time in microseconds to accrue deficit micro-tokens at one
-- token per T microseconds, rounded up.
local function tofull(deficit, T)
  local whole = idiv(deficit, MT)
  return whole * T + iceil((deficit - whole * MT) * T, MT)
end

local TOKENBUCKET = {}

function TOKENBUCKET.read(key, _)
  return redis.call('HMGET', key, 't', 'l')
end

function TOKENBUCKET.eval(v, base, n)
  local now = tonumber(ARGV[base + 2])
  local T = tonumber(ARGV[base + 3])
  local cap = tonumber(ARGV[base + 4])
  local capmt = cap * MT
  local tokens, last
  if v[1] then
    tokens = tonumber(v[1])
    last = tonumber(v[2])
  else
    tokens = capmt
    last = now
  end
  if now > last then
    local elapsed = now - last
    local whole = idiv(elapsed, T)
    if whole >= cap then
      tokens = capmt
    else
      tokens = tokens + whole * MT + idiv((elapsed - whole * T) * MT, T)
      if tokens > capmt then
        tokens = capmt
      end
    end
    last = now
  elseif tokens > capmt then
    tokens = capmt -- a clock that is behind sees no refill; last stays put
  end
  local need = n * MT
  if tokens < need then
    return { 0, idiv(tokens, MT), tofull(need - tokens, T), tofull(capmt - tokens, T), cap }, nil
  end
  tokens = tokens - need
  local reset = tofull(capmt - tokens, T)
  return { 1, idiv(tokens, MT), 0, reset, cap }, { tokens, last, reset }
end

function TOKENBUCKET.write(key, base, w)
  redis.call('HSET', key, 't', fmt(w[1]), 'l', fmt(w[2]))
  redis.call('PEXPIRE', key, px(w[3], tonumber(ARGV[base + 6])))
end

-- tokenbucket.lua: token bucket with lazy refill. State is a 2-field hash:
-- t = micro-tokens (one token = 1e6), l = last refill instant in unix
-- microseconds. A missing key is a full bucket. Refill is computed in two
-- integer parts (whole emission intervals, then the remainder scaled to
-- micro-tokens); whole >= burst short-circuits to full so no product
-- approaches 2^53. Last never moves backwards. Reference model: internal/algo/tokenbucket.Step.
local MT = 1000000

-- tofull is the time in microseconds to accrue deficit micro-tokens at one
-- token per T microseconds, rounded up.
local function tofull(deficit, T)
  local whole = idiv(deficit, MT)
  return whole * T + iceil((deficit - whole * MT) * T, MT)
end

local function tokenbucket(key, base, commit)
  local now = tonumber(ARGV[base + 2])
  local T = tonumber(ARGV[base + 3])
  local cap = tonumber(ARGV[base + 4])
  local n = tonumber(ARGV[base + 5])
  local slack = tonumber(ARGV[base + 6])
  local capmt = cap * MT
  local v = redis.call('HMGET', key, 't', 'l')
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
    return { 0, idiv(tokens, MT), tofull(need - tokens, T), tofull(capmt - tokens, T), cap }
  end
  tokens = tokens - need
  local reset = tofull(capmt - tokens, T)
  if commit == 1 then
    redis.call('HSET', key, 't', fmt(tokens), 'l', fmt(last))
    redis.call('PEXPIRE', key, px(reset, slack))
  end
  return { 1, idiv(tokens, MT), 0, reset, cap }
end

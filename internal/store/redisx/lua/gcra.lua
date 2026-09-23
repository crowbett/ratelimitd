-- gcra.lua: Generic Cell Rate Algorithm. State is one integer, the
-- theoretical arrival time (TAT) in unix microseconds, in a plain string key.
-- Reference model: internal/algo/gcra.Step.
--
-- Every algorithm file exposes the same three-function table so dispatch.lua
-- can read every key once, evaluate, and write only when the whole call
-- allows (single pass, buffered writes):
--   read(key, base)       -> state (whatever the algorithm needs; one read per key)
--   eval(state, base, n)  -> {allowed, remaining, retry_us, reset_us, limit}, write
--                            (write is nil on a denial: a denied call never writes)
--   write(key, base, w)   -> performs the buffered write
-- eval is pure over state and ARGV, so it may be called again with another n
-- (the lease path) without touching Redis.

-- remaining is floor(slack_us / T) clamped at 0 (negative only when Burst
-- shrank under a live key).
local function remaining(slack_us, T)
  if slack_us <= 0 then
    return 0
  end
  return idiv(slack_us, T)
end

local GCRA = {}

function GCRA.read(key, base)
  return tonumber(redis.call('GET', key)) or tonumber(ARGV[base + 2])
end

function GCRA.eval(tat, base, n)
  local now = tonumber(ARGV[base + 2])
  local T = tonumber(ARGV[base + 3])
  local cap = tonumber(ARGV[base + 4])
  local dvt = cap * T
  if tat < now then
    tat = now
  end
  local newtat = tat + n * T
  local diff = now - (newtat - dvt)
  if diff < 0 then
    return { 0, remaining(dvt - (tat - now), T), -diff, tat - now, cap }, nil
  end
  return { 1, remaining(dvt - (newtat - now), T), 0, newtat - now, cap }, newtat
end

function GCRA.write(key, base, newtat)
  local now = tonumber(ARGV[base + 2])
  redis.call('SET', key, fmt(newtat), 'PX', px(newtat - now, tonumber(ARGV[base + 6])))
end

-- gcra.lua: Generic Cell Rate Algorithm. State is one integer, the
-- theoretical arrival time (TAT) in unix microseconds, in a plain string key.
-- Reference model: internal/algo/gcra.Step.
--
-- Returns {allowed, remaining, retry_us, reset_us, limit}; a denied call
-- never writes.

-- remaining is floor(slack_us / T) clamped at 0 (negative only when Burst
-- shrank under a live key).
local function remaining(slack_us, T)
  if slack_us <= 0 then
    return 0
  end
  return idiv(slack_us, T)
end

local function gcra(key, base, commit)
  local now = tonumber(ARGV[base + 2])
  local T = tonumber(ARGV[base + 3])
  local cap = tonumber(ARGV[base + 4])
  local n = tonumber(ARGV[base + 5])
  local slack = tonumber(ARGV[base + 6])
  local dvt = cap * T
  local tat = tonumber(redis.call('GET', key)) or now
  if tat < now then
    tat = now
  end
  local newtat = tat + n * T
  local diff = now - (newtat - dvt)
  if diff < 0 then
    return { 0, remaining(dvt - (tat - now), T), -diff, tat - now, cap }
  end
  if commit == 1 then
    redis.call('SET', key, fmt(newtat), 'PX', px(newtat - now, slack))
  end
  return { 1, remaining(dvt - (newtat - now), T), 0, newtat - now, cap }
end

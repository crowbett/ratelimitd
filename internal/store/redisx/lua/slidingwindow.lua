-- slidingwindow.lua: the weighted two-window approximation (hash p/c/i =
-- previous count, current count, window index) and the exact
-- ring-of-timestamps variant (a ZSET scored by unix microseconds). Both use
-- rate per period_us as the limit; burst is ignored. Reference models:
-- internal/algo/slidingwindow.Step and StepExact. Table shape
-- (read/eval/write): see gcra.lua.

-- sw_retry is the time until the weighted previous count has decayed by
-- excess/W events, capped at the window boundary (after which cur rolls over
-- and the caller re-asks). excess is scaled by W.
local function sw_retry(excess, prev, to_boundary)
  if prev > 0 then
    local r = iceil(excess, prev)
    if r < to_boundary then
      if r < 1 then
        r = 1
      end
      return r
    end
  end
  return to_boundary
end

-- sw_reset is the microseconds until both counters are irrelevant.
local function sw_reset(prev, cur, elapsed, W)
  if cur > 0 then
    return 2 * W - elapsed
  end
  if prev > 0 then
    return W - elapsed
  end
  return 0
end

-- sw_remaining is floor(avail / W) clamped at 0.
local function sw_remaining(avail, W)
  if avail <= 0 then
    return 0
  end
  return idiv(avail, W)
end

local SLIDINGWINDOW = {}

function SLIDINGWINDOW.read(key, _)
  return redis.call('HMGET', key, 'p', 'c', 'i')
end

function SLIDINGWINDOW.eval(v, base, n)
  local now = tonumber(ARGV[base + 2])
  local limit = tonumber(ARGV[base + 7])
  local W = tonumber(ARGV[base + 8])
  local idx = idiv(now, W)
  local elapsed = now - idx * W
  local prev, cur = 0, 0
  if v[1] then
    local sp, sc, si = tonumber(v[1]), tonumber(v[2]), tonumber(v[3])
    if idx == si then
      prev, cur = sp, sc
    elseif idx == si + 1 then
      prev, cur = sc, 0
    elseif idx < si then
      -- the caller's clock is behind the stored window: pin to its start
      prev, cur, idx, elapsed = sp, sc, si, 0
    end
  end
  local avail = limit * W - prev * (W - elapsed) - cur * W -- scaled by W
  if n * W > avail then
    return { 0, sw_remaining(avail, W), sw_retry(n * W - avail, prev, W - elapsed), sw_reset(prev, cur, elapsed, W), limit }, nil
  end
  cur = cur + n
  return { 1, sw_remaining(avail - n * W, W), 0, sw_reset(prev, cur, elapsed, W), limit }, { prev, cur, idx }
end

function SLIDINGWINDOW.write(key, base, w)
  local now = tonumber(ARGV[base + 2])
  local W = tonumber(ARGV[base + 8])
  redis.call('HSET', key, 'p', fmt(w[1]), 'c', fmt(w[2]), 'i', fmt(w[3]))
  redis.call('PEXPIRE', key, px((w[3] + 2) * W - now, tonumber(ARGV[base + 6])))
end

-- The exact variant's reads depend on n (the k-th oldest live stamp on a
-- denial), so read only carries the key and eval issues the reads. It is
-- still one evaluation per key per call; the lease path rejects it.
local SLIDINGWINDOW_EXACT = {}

function SLIDINGWINDOW_EXACT.read(key, _)
  return key
end

function SLIDINGWINDOW_EXACT.eval(key, base, n)
  local now = tonumber(ARGV[base + 2])
  local limit = tonumber(ARGV[base + 7])
  local W = tonumber(ARGV[base + 8])
  local floor_ts = now - W -- a stamp is live while stamp > now - W
  local xmin = '(' .. fmt(floor_ts)
  local count = redis.call('ZCOUNT', key, xmin, '+inf')
  local newest = now
  local reset = 0
  if count > 0 then
    local top = redis.call('ZREVRANGE', key, 0, 0, 'WITHSCORES')
    newest = tonumber(top[2])
    reset = newest + W - now
  end
  if count + n > limit then
    local retry = W
    if n <= limit then
      local k = count + n - limit -- stamps that must expire first
      local r = redis.call('ZRANGEBYSCORE', key, xmin, '+inf', 'WITHSCORES', 'LIMIT', k - 1, 1)
      retry = tonumber(r[2]) + W - now
      if retry < 1 then
        retry = 1
      end
    end
    local rem = limit - count
    if rem < 0 then
      rem = 0
    end
    return { 0, rem, retry, reset, limit }, nil
  end
  local stamp = now
  if newest > stamp then
    stamp = newest -- clock behind: never break the ordering
  end
  return { 1, limit - count - n, 0, stamp + W - now, limit }, { stamp, floor_ts, n }
end

function SLIDINGWINDOW_EXACT.write(key, base, w)
  local now = tonumber(ARGV[base + 2])
  local W = tonumber(ARGV[base + 8])
  local nonce = ARGV[base + 9]
  local stamp = w[1]
  redis.call('ZREMRANGEBYSCORE', key, '-inf', fmt(w[2]))
  local sstamp = fmt(stamp)
  for i = 1, w[3] do
    redis.call('ZADD', key, sstamp, sstamp .. ':' .. nonce .. ':' .. i)
  end
  redis.call('PEXPIRE', key, px(stamp + W - now, tonumber(ARGV[base + 6])))
end

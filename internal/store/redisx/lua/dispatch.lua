-- dispatch.lua: the entry point of the concatenated script. KEYS holds one
-- state key per request (all in one hash slot: the caller's keys share the
-- {t=<tenant>|s=<subject>} tag), ARGV holds NARGV entries per key and a
-- trailing mode flag:
--
--   '1' apply: SINGLE PASS. Every key is read once and evaluated against the
--       state it had when the call started; the writes are buffered. Only if
--       every key allowed are the buffered writes performed, so a denial
--       never mutates state (docs/PLAN.md, Decision 4). The reply for an
--       overall denial is exactly what each key evaluated to (allowed keys
--       report the view they would have committed), as before. A key listed
--       twice is evaluated twice against the same pre-call state and the
--       later write wins, the same as the local backend; Resolve never
--       emits duplicates (policy names are unique per request).
--   '0' peek: the same evaluation, never writes.
--   'L' lease (one key; gcra, token_bucket, sliding_window): grant between
--       min = ARGV[5] (the cost slot) and want = ARGV[9] (the nonce slot)
--       units in one step: want if it fits, else as many as fit if that is
--       >= min, else a denial evaluated at min. Reply is the 5-integer
--       decision plus the granted count (0 on a denial).
--
-- The flat reply is {allowed, remaining, retry_us, reset_us, limit} * n.
local ALGO = { ['1'] = GCRA, ['2'] = TOKENBUCKET, ['3'] = SLIDINGWINDOW, ['4'] = SLIDINGWINDOW_EXACT }
local function run(KEYS, argv)
  ARGV = argv
  local nkeys = #KEYS
  if #ARGV ~= nkeys * NARGV + 1 then
    return redis.error_reply('ratelimitd: ARGV length ' .. #ARGV .. ' != ' .. (nkeys * NARGV + 1))
  end
  for k = 1, nkeys do
    local algo = ARGV[(k - 1) * NARGV + 1]
    if ALGO[algo] == nil then
      return redis.error_reply('ratelimitd: unknown algorithm ' .. tostring(algo))
    end
  end
  local mode = ARGV[nkeys * NARGV + 1]

  if mode == 'L' then
    if nkeys ~= 1 or ARGV[1] == '4' then
      return redis.error_reply('ratelimitd: lease takes one gcra, token_bucket or sliding_window key')
    end
    local a = ALGO[ARGV[1]]
    local lo, want = tonumber(ARGV[5]), tonumber(ARGV[9])
    if want < lo then
      want = lo
    end
    local st = a.read(KEYS[1], 0)
    local r, w = a.eval(st, 0, want)
    local got = want
    if r[1] == 0 then
      if r[2] >= lo then
        got = r[2]
        r, w = a.eval(st, 0, got)
      else
        got = lo
        r, w = a.eval(st, 0, lo)
      end
      if r[1] == 0 then
        got = 0
      end
    end
    if got > 0 then
      a.write(KEYS[1], 0, w)
    end
    r[6] = got
    return r
  end

  if mode ~= '0' and mode ~= '1' then
    return redis.error_reply('ratelimitd: unknown mode ' .. tostring(mode))
  end
  local out, writes, all = {}, {}, true
  for k = 1, nkeys do
    local base = (k - 1) * NARGV
    local a = ALGO[ARGV[base + 1]]
    local r, w = a.eval(a.read(KEYS[k], base), base, tonumber(ARGV[base + 5]))
    if r[1] == 0 then
      all = false
    end
    writes[k] = w
    local o = (k - 1) * 5
    for i = 1, 5 do
      out[o + i] = r[i]
    end
  end
  if all and mode == '1' then
    for k = 1, nkeys do
      local base = (k - 1) * NARGV
      ALGO[ARGV[base + 1]].write(KEYS[k], base, writes[k])
    end
  end
  return out
end

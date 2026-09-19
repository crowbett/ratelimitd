-- dispatch.lua: the entry point of the concatenated script. KEYS holds one
-- state key per request (all in one hash slot: the caller's keys share the
-- {t=<tenant>|s=<subject>} tag), ARGV holds NARGV entries per key and a
-- trailing commit flag. Pass 1 evaluates every key with commit=0; only if
-- every key allowed AND the flag is '1' does pass 2 re-run with commit=1
-- (docs/PLAN.md, Decision 4: a denial never mutates state). The reply is a
-- flat integer array {allowed, remaining, retry_us, reset_us, limit} * n.
local nkeys = #KEYS
if #ARGV ~= nkeys * NARGV + 1 then
  return redis.error_reply('ratelimitd: ARGV length ' .. #ARGV .. ' != ' .. (nkeys * NARGV + 1))
end
for k = 1, nkeys do
  local algo = ARGV[(k - 1) * NARGV + 1]
  if algo ~= '1' and algo ~= '2' and algo ~= '3' and algo ~= '4' then
    return redis.error_reply('ratelimitd: unknown algorithm ' .. tostring(algo))
  end
end
local out = {}
local function pass(commit)
  local all = 1
  for k = 1, nkeys do
    local base = (k - 1) * NARGV
    local algo = ARGV[base + 1]
    local r
    if algo == '1' then
      r = gcra(KEYS[k], base, commit)
    elseif algo == '2' then
      r = tokenbucket(KEYS[k], base, commit)
    elseif algo == '3' then
      r = slidingwindow(KEYS[k], base, commit)
    else
      r = slidingwindow_exact(KEYS[k], base, commit)
    end
    if r[1] == 0 then
      all = 0
    end
    local o = (k - 1) * 5
    for i = 1, 5 do
      out[o + i] = r[i]
    end
  end
  return all
end
-- One key needs no dry run: every algorithm already leaves state untouched on
-- a denial, so the commit pass alone is the all-or-nothing rule for n = 1.
if nkeys == 1 and ARGV[NARGV + 1] == '1' then
  pass(1)
elseif pass(0) == 1 and ARGV[nkeys * NARGV + 1] == '1' then
  pass(1)
end
return out

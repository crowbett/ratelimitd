-- common.lua: integer helpers shared by every algorithm file. The files are
-- concatenated at init (common, gcra, tokenbucket, slidingwindow, dispatch)
-- into ONE script, so these chunk-level locals are visible to all of them.
--
-- Every value crossing this boundary is an integer (unix microseconds,
-- micro-tokens, counts). Lua numbers are IEEE doubles, exact to 2^53, and the
-- Go reference models (internal/algo/*) use the same integer arithmetic; the
-- differential test in internal/store/redisx asserts they never diverge.

-- fmt renders an integer-valued double without the "%.14g" exponent form that
-- Redis would otherwise use when a number is passed as a command argument
-- (2^52 would become "4.5035996273705e+15" and lose digits).
local function fmt(v)
  return string.format('%.0f', v)
end

-- idiv is floor(a / b) computed so the result is exact for integers even
-- when the double quotient rounds across an integer boundary.
local function idiv(a, b)
  local q = math.floor(a / b)
  if q * b > a then
    q = q - 1
  elseif (q + 1) * b <= a then
    q = q + 1
  end
  return q
end

-- iceil is ceil(a / b) for b > 0.
local function iceil(a, b)
  return -idiv(-a, b)
end

-- px turns a state TTL in microseconds plus the configured slack (ms) into a
-- PX argument, never below 1 ms.
local function px(ttl_us, slack_ms)
  local ms = iceil(ttl_us, 1000) + slack_ms
  if ms < 1 then
    ms = 1
  end
  return ms
end

-- argv layout: every key owns NARGV consecutive ARGV entries, then one
-- trailing commit flag ('0' | '1') for the whole call.
--   base+1 algo (1 gcra, 2 token_bucket, 3 sliding_window, 4 sliding_window_exact)
--   base+2 now_us      base+3 emission_us   base+4 burst
--   base+5 cost        base+6 ttl_slack_ms  base+7 rate
--   base+8 period_us   base+9 nonce (exact variant member uniqueness)
local NARGV = 9

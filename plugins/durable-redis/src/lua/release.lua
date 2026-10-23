-- Shared by ack and nack. KEYS[1] = scope prefix
-- ARGV = mode ('ack' | 'nack'), run, token, at_ms (-1 = none)
-- Returns 1 on success, 0 when the token is stale (the lease was lost).
local P = KEYS[1]
local function s(n) return string.format('%.0f', n) end
local mode, run, token, at = ARGV[1], ARGV[2], ARGV[3], tonumber(ARGV[4])
local ik = P .. ':item:' .. run
if redis.call('HGET', ik, 'state') ~= 'l' or redis.call('HGET', ik, 'token') ~= token then
  return 0
end
local ckey = redis.call('HGET', ik, 'ckey')
redis.call('ZREM', P .. ':inflight', run)
if ckey and ckey ~= '' then redis.call('ZREM', P .. ':inflight:k:' .. ckey, run) end

local rerun = redis.call('HGET', ik, 'rerun_due')
local due, kind = nil, redis.call('HGET', ik, 'kind')
if rerun then
  due = tonumber(rerun)
  kind = 'resume'
end
if mode == 'ack' then
  -- Reschedule at the next wake time; nothing to run means the item is done.
  if at >= 0 then
    due = due and math.min(due, at) or at
    kind = 'resume'
  end
  if due == nil then
    redis.call('DEL', ik)
    return 1
  end
else
  -- nack: retry later, but a pending rerun must not wait out the delay.
  due = due and math.min(due, at) or at
end
redis.call('HSET', ik, 'state', 'q', 'due', s(due), 'kind', kind)
redis.call('HDEL', ik, 'token', 'expires', 'rerun_due')
redis.call('ZADD', P .. ':q:' .. kind, due, run)
return 1

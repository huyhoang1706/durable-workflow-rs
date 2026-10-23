-- KEYS[1] = key; ARGV = now_ms, limit, period_ms
-- Returns {1, 0} when allowed, {0, retry_at_ms} when denied.
local function s(n) return string.format('%.0f', n) end
local now, limit, period = tonumber(ARGV[1]), tonumber(ARGV[2]), tonumber(ARGV[3])
local emission = math.floor(period / math.max(limit, 1))
local tat = tonumber(redis.call('GET', KEYS[1])) or now
if tat < now then tat = now end
local allow_at = tat - (period - emission)
if now >= allow_at then
  redis.call('SET', KEYS[1], s(tat + emission), 'PX', s(period * 2))
  return { 1, 0 }
end
return { 0, allow_at }

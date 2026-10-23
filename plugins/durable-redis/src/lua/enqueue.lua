-- KEYS[1] = scope prefix (hash-tagged, so every key below is in one slot)
-- ARGV    = repeated [run, due_ms, kind, concurrency_key, throttle_key, item_json]
local P = KEYS[1]
local function s(n) return string.format('%.0f', n) end

for i = 1, #ARGV, 6 do
  local run, due, kind = ARGV[i], tonumber(ARGV[i + 1]), ARGV[i + 2]
  local ckey, tkey, json = ARGV[i + 3], ARGV[i + 4], ARGV[i + 5]
  local ik = P .. ':item:' .. run
  local state = redis.call('HGET', ik, 'state')
  if not state then
    redis.call('HSET', ik, 'state', 'q', 'due', s(due), 'kind', kind,
               'ckey', ckey, 'tkey', tkey, 'json', json)
    redis.call('ZADD', P .. ':q:' .. kind, due, run)
  elseif state == 'q' then
    -- Dedupe per run: earliest due wins, and a resume upgrades a start.
    local cur_due = tonumber(redis.call('HGET', ik, 'due'))
    local cur_kind = redis.call('HGET', ik, 'kind')
    local new_due = math.min(cur_due, due)
    local new_kind = (cur_kind == 'resume' or kind == 'resume') and 'resume' or 'start'
    if new_kind ~= cur_kind then redis.call('ZREM', P .. ':q:' .. cur_kind, run) end
    redis.call('HSET', ik, 'due', s(new_due), 'kind', new_kind)
    redis.call('ZADD', P .. ':q:' .. new_kind, new_due, run)
  else
    -- Leased: remember a rerun so ack/nack/expiry re-enqueue it.
    local rerun = redis.call('HGET', ik, 'rerun_due')
    local d = rerun and math.min(tonumber(rerun), due) or due
    redis.call('HSET', ik, 'rerun_due', s(d))
  end
end
return 1

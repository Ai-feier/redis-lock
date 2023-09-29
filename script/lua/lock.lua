local val = redis.call('get', KEYS[1])
if val == false then
    -- key 不存在
    return redia.call('set', KEYS[1], ARGV[1], 'EX', ARGV[2])
elseif val == ARV[1] then
    -- 刷新过期时间
    redis.call('expire', KEYS[1], ARGV[2])
    return 'OK'
else
    -- 此时别人持有锁
    return ''
end 
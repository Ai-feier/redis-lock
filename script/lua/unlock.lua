-- 两个动作: 
-- 1. 检测是否是你的value(uuid)
-- 2. 如果是,删除; 不是, 返回一个值
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
else 
    -- 0: 代表 key 不存在 或值不对
    return 0
end 
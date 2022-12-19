const redis = require('redis');

const KEYPRE = 'TINYTASK';
var redisClient = null;

async function init(host, port) {
    if (redisClient) return;
    redisClient = redis.createClient({
        host: host || '127.0.0.1', port: port || 6379, database: 0
    });
    await redisClient.connect();
    await redisClient.configSet("notify-keyspace-events", "Ex");

    const subscriber = redisClient.duplicate();
    await subscriber.connect();
    subscriber.subscribe('__keyevent@0__:expired', async (msg) => {
        let key = msg;
        let xr = key.split(':');
        if (xr[0] != KEYPRE) return;

        let lock = await redisClient.set(key, 'L', { NX: true });
        if (!lock) return;  // Mutex, 避免被多个接收者多次执行

        let key2 = `${KEYPRE}2:${xr[1]}`;
        let data = await redisClient.get(key2);

        // consume here
        console.log('task', xr[1], data);

        await redisClient.del(key);
        await redisClient.del(key2);
    });
}

async function add(delaySecond, taskId, taskData) {
    let key = `${KEYPRE}:${taskId}`;
    await redisClient.set(key, '1');
    await redisClient.expire(key, delaySecond);

    let key2 = `${KEYPRE}2:${taskId}`;
    await redisClient.set(key2, taskData);
    await redisClient.expire(key2, delaySecond + 100);

    console.log('New Task', taskId, 'after', delaySecond)
}


/////////////////////// TEST ///////////////////////////
(async () => {
    await init();
    await add(2, 'dodo1', '222222');
    await add(5, 'dodo2', '555555');
})();
/////////////////////// TEST ///////////////////////////
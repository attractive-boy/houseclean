const { MongoClient } = require('mongodb');

let client = null; // 声明一个变量来存储 MongoClient 实例

async function initializeClient() {
    if (!client) { // 如果客户端尚未初始化
        client = new MongoClient('mongodb://localhost:27017'); // 连接到 MongoDB
        await client.connect(); // 连接数据库
    }
    return client; // 返回客户端实例
}

initializeClient(); // 初始化客户端

module.exports = {
    client
}; 
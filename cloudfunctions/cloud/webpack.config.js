const path = require('path');
const NodePolyfillPlugin = require("node-polyfill-webpack-plugin");

module.exports = {
    entry: './application.js',
    output: {
        filename: 'bundle.js', // 输出文件名
        path: path.resolve(__dirname, 'dist'), // 输出路径
        publicPath: '/dist/' // 公开路径
    },
    module: {
        rules: [
            {
                test: /\.js$/, // 匹配所有 .js 文件
                exclude: /node_modules/, // 排除 node_modules 目录
                use: {
                    loader: 'babel-loader', // 使用 Babel 加载器
                    options: {
                        presets: ['@babel/preset-env'] // Babel 预设
                    }
                }
            }
        ]
    },
    resolve: {
        fallback: {
            "http": require.resolve("stream-http"),
            "https": require.resolve("https-browserify"),
            "stream": require.resolve("stream-browserify"),
            "zlib": require.resolve("browserify-zlib"),
            "crypto": require.resolve("crypto-browserify"),
            "path": require.resolve("path-browserify"),
            "os": require.resolve("os-browserify/browser"),
            "timers": require.resolve("timers-browserify"),
            "tty": require.resolve("tty-browserify"),
            "vm": require.resolve("vm-browserify"),
            "constants": require.resolve("constants-browserify"),
            "module": false, // 直接禁用 module 模块
            "url": false, // 直接禁用 url 模块
            "util": false, // 直接禁用 util 模块
        }
    },
    devtool: 'source-map', // 生成 source map
    mode: 'development', // 开发模式
    plugins: [
        new NodePolyfillPlugin() // 引入 NodePolyfillPlugin 插件
    ]
}; 
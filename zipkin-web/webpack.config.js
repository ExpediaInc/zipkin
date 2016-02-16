var webpack = require('webpack');
var ExtractTextPlugin = require('extract-text-webpack-plugin');

module.exports = {
    entry: [
        __dirname + '/src/main/resources/app/js/main.js',
        __dirname + '/src/main/resources/app/css/style-loader.js',
    ],
    resolve: {
        alias: { flight: 'flightjs', chosen: 'chosen-npm/public/chosen.jquery.js' },
        modulesDirectories: ['node_modules']
    },
    module: {
        loaders: [{
            test: /\.js$/,
            exclude: /node_modules/,
            loader: 'babel?presets[]=es2015'
        }, {
            test: /\.mustache$/,
            loader: 'mustache'
        }, {
            test: /.scss$/,
            loader: ExtractTextPlugin.extract('style-loader', 'css-loader!sass-loader')
        }, {
            test: /\.woff2?$|\.ttf$|\.eot$|\.svg|\.png$/,
            loader: 'file'
        }]
    },
    output: {
        path: __dirname + '/src/main/resources/dist',
        filename: 'app.min.js',
        publicPath: '/dist/'
    },
    plugins: [
        new webpack.ProvidePlugin({
            $: "jquery",
            jQuery: "jquery"
        }),
        new ExtractTextPlugin("app.min.css", {allChunks: true})
    ],
    devServer: {
        port: 9090,
        proxy: {
            "*": "http://localhost:8080"
        }
    }
};

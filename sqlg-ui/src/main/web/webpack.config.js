const path = require('path');
let webpack = require('webpack');

module.exports = {
    mode: 'development',
    entry: {
        app: './sqlg/v1/src/index.js'
    },
    devtool: 'inline-source-map',
    plugins: [
        new webpack.ProvidePlugin({
            "$": "jquery",
            "jQuery": "jquery"
        })
    ],
    output: {
        path: path.resolve(__dirname, 'dist/sqlg/v1'),
        filename: 'app.js',
        clean: true,
    },
    module: {
        rules: [
            {
                test: /(\.png|\.gif|\.ttf|\.eot|\.woff|\.svg|\.jpg|\.jpe?g)/,
                use: [
                    {loader: "file-loader", options: {outputPath: 'assets'}}
                ]
            },
            {test: /\.css$/, use: ['style-loader', 'css-loader']},
            {
                test: /\.s[ac]ss$/i,
                use: [
                    // Creates `style` nodes from JS strings
                    "style-loader",
                    // Translates CSS into CommonJS
                    "css-loader",
                    // Compiles Sass to CSS
                    "sass-loader",
                ],
            },
            {
                test: /jqx-all\.js$/,
                use: ['script-loader']
            }
        ],
    }
};
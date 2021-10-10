const path = require('path');
let webpack = require('webpack');
const HtmlWebpackPlugin = require('html-webpack-plugin');

module.exports = {
    mode: 'development',
    entry: {
        app: './sqlg/v1/src/index.js'
    },
    devtool: 'eval-source-map',
    plugins: [
        new webpack.ProvidePlugin({
            "$": "jquery",
            "jQuery": "jquery",
            Popper: ['popper.js', 'default']
        }),
        new HtmlWebpackPlugin({favicon: "sqlg/v1/src/favicon/favicon-16x16.png"})
    ],
    output: {
        path: path.resolve(__dirname, 'dist/dist/sqlg/v1'),
        filename: 'sqlg.js',
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
                test: /\.(scss)$/,
                use: [{
                    // inject CSS to page
                    loader: 'style-loader'
                }, {
                    // translates CSS into CommonJS modules
                    loader: 'css-loader'
                }, {
                    // Run postcss actions
                    loader: 'postcss-loader',
                    options: {
                        // `postcssOptions` is needed for postcss 8.x;
                        // if you use postcss 7.x skip the key
                        postcssOptions: {
                            // postcss plugins, can be exported to postcss.config.js
                            plugins: function () {
                                return [
                                    require('autoprefixer')
                                ];
                            }
                        }
                    }
                }, {
                    // compiles Sass to CSS
                    loader: 'sass-loader'
                }]
            },
            // {
            //     test: /\.s[ac]ss$/i,
            //     use: [
            //         // Creates `style` nodes from JS strings
            //         "style-loader",
            //         // Translates CSS into CommonJS
            //         "css-loader",
            //         // Compiles Sass to CSS
            //         "sass-loader",
            //     ],
            // },
            {
                test: /jqx-all\.js$/,
                use: ['script-loader']
            }
        ],
    }
};
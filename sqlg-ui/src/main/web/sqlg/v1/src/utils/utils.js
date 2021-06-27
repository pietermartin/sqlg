import $ from 'jquery';

const Utils = {

    HTTP_STATUS_CODES: Object.freeze({
        200: 'OK',
        201: 'Created',
        202: 'Accepted',
        203: 'Non-Authoritative Information',
        204: 'No Content',
        205: 'Reset Content',
        206: 'Partial Content',
        300: 'Multiple Choices',
        301: 'Moved Permanently',
        302: 'Found',
        303: 'See Other',
        304: 'Not Modified',
        305: 'Use Proxy',
        307: 'Temporary Redirect',
        400: 'Bad Request',
        401: 'Unauthorized',
        402: 'Payment Required',
        403: 'Forbidden',
        404: 'Not Found',
        405: 'Method Not Allowed',
        406: 'Not Acceptable',
        407: 'Proxy Authentication Required',
        408: 'Request Timeout',
        409: 'Conflict',
        410: 'Gone',
        411: 'Length Required',
        412: 'Precondition Failed',
        413: 'Request Entity Too Large',
        414: 'Request-URI Too Long',
        415: 'Unsupported Media Type',
        416: 'Requested Range Not Satisfiable',
        417: 'Expectation Failed',
        500: 'Internal Server Error',
        501: 'Not Implemented',
        502: 'Bad Gateway',
        503: 'Service Unavailable',
        504: 'Gateway Timeout',
        505: 'HTTP Version Not Supported'
    }),

    SEARCH_ENUM: Object.freeze(
        {
            contains: {
                name: "contains",
                description: "Contains case sensitive",
                icon: "fas fa-font-case"
            },
            ncontains: {
                name: "ncontains",
                description: "Not contains case sensitive",
                icon: "far fa-font-case"
            },
            containsCIS: {
                name: "containsCIS",
                description: "Contains case insensitive",
                icon: "fas fa-stars"
            },
            ncontainsCIS: {
                name: "ncontainsCIS",
                description: "Not contains case insensitive",
                icon: "far fa-stars"
            },
            startsWith: {
                name: "startsWith",
                description: "Starts with",
                icon: "fas fa-arrow-from-left"
            },
            endsWith: {
                name: "endsWith",
                description: "Ends with",
                icon: "fas fa-arrow-from-right"
            },
            equals: {
                name: "equals",
                description: "Equals",
                icon: "fas fa-equals"
            },
            nequals: {
                name: "nequals",
                description: "Not equals",
                icon: "fas fa-not-equal"
            },
            greaterThan: {
                name: "greater",
                description: "Greater than",
                icon: "fas fa-greater-than"
            },
            greaterThanOrEqual: {
                name: "greaterEqual",
                description: "Greater than or equal",
                icon: "far fa-greater-than-equal"
            },
            smallerThan: {
                name: "small",
                description: "Smaller than",
                icon: "fas fa-less-than"
            },
            smallerThanOrEqual: {
                name: "smallEqual",
                description: "Smaller than or equal",
                icon: "far fa-less-than-equal"
            },
        }),

    jq: function (myid) {
        return myid.replace(/(:|\.|\[|\]|,|=|@)/g, "\\$1");
    },

    uuidv4: function () {
        return ([1e7] + -1e3 + -4e3 + -8e3 + -1e11).replace(/[018]/g, c =>
            (c ^ crypto.getRandomValues(new Uint8Array(1))[0] & 15 >> c / 4).toString(16)
        );
    },

    escapeHtml: function (string) {
        let entityMap = {
            '&': '&amp;',
            '<': '_',
            '>': '_',
            '"': '&quot;',
            "'": '&#39;',
            '/': '&#x2F;',
            '`': '&#x60;',
            '=': '&#x3D;'
        };
        return String(string).replace(/[&<>"'`=\/]/g, function (s) {
            return entityMap[s];
        });
    },

    ExportToCSVConverterAsync: function (JSONData, FileTitle, ShowLabel) {
        var d = $.Deferred(function (d) {
            // we break at the maximum allowed byte length e.g. 150MB for chrome and firefox and 50MB for IE
            var isIE = (/(Trident\/[7]{1})/i.test(navigator.userAgent)) || (/(MSIE\ [0-9]{1})/i.test(navigator.userAgent));
            var maxBytesAllowed = (isIE) ? 52000000 : 157200000;
            //If JSONData is not an object then JSON.parse will parse the JSON string in an Object
            var arrData = typeof JSONData != 'object' ? JSON.parse(JSONData) : JSONData;
            JSONData = null;
            //console.info(arrData);
            var CSV = '';
            var dt = new Date();
            var date = dt.toDateString().split(' ').join('-');
            var time = dt.getHours() + "" + dt.getMinutes() + "" + dt.getSeconds();
            //Generate a file name
            var fileName = "CMexport_";
            //this will remove the blank-spaces from the title and replace it with an underscore
            fileName += FileTitle.replace(/ /g, "_");
            fileName += '-' + date + '_' + time;
            //This condition will generate the Label/Header
            if (ShowLabel) {
                var row = "";
                //This loop will extract the label from 1st index of on array
                for (var index in arrData[0]) {
                    //Now convert each value to string and comma-seprated
                    row += index + ',';
                }
                row = row.slice(0, -1);
                //append Label row with line break
                CSV += row + '\r\n';
            }
            //1st loop is to extract each row
            console.log('starting object extraction for csv....');
            var length = arrData.length;
            var totalExported = 0;
            for (var i = 0; i < length; i++) {
                var row = "";
                //2nd loop will extract each column and convert it in string comma-seprated
                for (var index in arrData[i]) {
                    //row += '"' + arrData[i][index] + '",';
                    row += arrData[i][index] + ',';
                }
                row.slice(0, row.length - 1);
                //add a line break after each row
                CSV += row + '\r\n';
                if (CSV.length >= maxBytesAllowed) {
                    totalExported = i;
                    i = arrData.length + 5;
                    console.log('Maximum allowed bytes reached.  Truncing the results at row ' + totalExported + '.');
                }
            }
            if (CSV == '') {
                alert("Invalid data");
                return;
            }
            //Set Report title in first row or line
            totalExported = (totalExported == 0) ? (length) : totalExported;
            var toppart = FileTitle + '\r\n\n';
            toppart += 'Total records: ' + length + '\r\n\n';
            toppart += 'Records in this file: ' + totalExported + '\r\n\n';
            //CSV = toppart + CSV;
            console.log('done with object extraction...');
            //Initialize file format you want csv or xls
            console.log('creating csv blob....');
            var $blob = new Blob([CSV], {type: 'text/csv;charset=utf-8;'});
            console.log('The blob size is ' + $blob.size);
            console.log('creating blob uri.......');
            var blobUrl = window.URL.createObjectURL($blob);
            var link = document.createElement('a');
            link.href = blobUrl;
            // link.innerHTML = 'Click to download ' + fileName + '.csv';
            link.download = fileName + '.csv';
            if (isIE) {
                document.getElementById('pager').appendChild(link);
                window.navigator.msSaveOrOpenBlob($blob, link.download);
                console.log('this is IE...');
            } else {
                var blobUrl = window.URL.createObjectURL($blob);
                document.body.appendChild(link);
                setTimeout(function () {
                    link.click();
                }, 1000);
            }
            setTimeout(function () {
                $blob = null;
                console.log('clearing blob url...');
                window.URL.revokeObjectURL(blobUrl);
                link.parentNode.removeChild(link);
            }, 3000);
            console.log('Good, download of ' + link.download + ' should start in a second....');
            d.resolve("OK");
        });
        return d.promise();
    },

    humanFileSize: function (bytes, si) {
        let thresh = si ? 1000 : 1024;
        if (Math.abs(bytes) < thresh) {
            return bytes + ' B';
        }
        let units = si
            ? ['kB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB']
            : ['KiB', 'MiB', 'GiB', 'TiB', 'PiB', 'EiB', 'ZiB', 'YiB'];
        let u = -1;
        do {
            bytes /= thresh;
            ++u;
        } while (Math.abs(bytes) >= thresh && u < units.length - 1);
        return bytes.toFixed(1) + ' ' + units[u];
    },

    isNumeric: function (value) {
        return !isNaN(parseFloat(n)) && isFinite(n);
    },

    uniqueId: function () {
        // always start with a letter (for DOM friendlyness)
        let idstr = String.fromCharCode(Math.floor((Math.random() * 25) + 65));
        do {
            // between numbers and characters (48 is 0 and 90 is Z (42-48 = 90)
            let ascicode = Math.floor((Math.random() * 42) + 48);
            if (ascicode < 58 || ascicode > 64) {
                // exclude all chars between : (58) and @ (64)
                idstr += String.fromCharCode(ascicode);
            }
        } while (idstr.length < 32);
        return idstr;
    },


};

export default Utils;